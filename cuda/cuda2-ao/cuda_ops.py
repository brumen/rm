#
# GPUArrayAO - an extension of the GPUArray class for AirOptions.
import numpy as np
import os.path

import pycuda.autoinit  # IMPORTANT: DO NOT REMOVE, this has to be here.
import pycuda.driver as drv
import pycuda.gpuarray as gpa
from pycuda.gpuarray import GPUArray
import pycuda.reduction
from pycuda.compiler import SourceModule
from pycuda.elementwise import ElementwiseKernel
import skcuda.cublas as cublas  # skcuda bindings to cublas
# import cublas

import config


class GPUArrayAO(GPUArray):
    """ Subclass of GPU array used for air-options.
    """


    gpu_set_const_float_k = ElementwiseKernel('float *m_new, float a',
                                              'm_new[i] = a;',
                                              'gpu_set_const_float_k')

    gpu_set_const_double_k = ElementwiseKernel('double *m_new, double a',
                                               'm_new[i] = a;',
                                               'gpu_set_const_double_k')

    def sum(self):
        return gpa.sum(self)

    @classmethod
    def zeros(cls, shape, dtype, allocator=drv.mem_alloc, order="C"):
        """Returns an array of the given shape and dtype filled with 0's."""
        result = cls(shape, dtype, allocator, order=order)
        result.fill(np.zeros((), dtype))
        return result

    @classmethod
    def empty(cls, shape, dtype, allocator=drv.mem_alloc, base=None, gpudata=None, strides=None, order="C"):
        """ Constructor of the empty GPUArrayAO.

        """

        return cls(shape, dtype, allocator=allocator, base=base, gpudata=gpudata, strides=strides, order=order)

    def set_mat_by_vec(self, v, nb_cols):
        """
        set the matrix by column vector
        only useful if number of rows (len of v) is much smaller than nb_cols
        """

        m_new = gpa.empty((len(v), nb_cols), dtype=np.double)
        for row_nb in range(len(v)):
            gpu_set_const_double_k(m_new[row_nb, :], v[row_nb])

        return m_new



# vector times vector - constructing a matrix, first vec is column, second is row

vtvpm_code = open(os.path.join(config.work_dir, 'cuda', 'vtvpm.c'), 'r').read()
# vector + vector function on rows
vtvpm_module = SourceModule(vtvpm_code)
vpv_f = vtvpm_module.get_function("vpv")
vtv_f = vtvpm_module.get_function("vtv")
vpv_double_f = vtvpm_module.get_function("vpv_double")
vtv_double_f = vtvpm_module.get_function("vtv_double")
vpv_double_f_slow = vtvpm_module.get_function("vpv_double_slow")
vtv_double_f_slow = vtvpm_module.get_function("vtv_double_slow")


def vtpv_old(v1, v2, tm_ind='p', transpose_ind=False):
    """
    WORSE VERSION OF THE FUNCTION BELOW
    vector times/plus vector - constructs a matrix
    :param v1, v2: 2 vectors, first serves as column vector, second as row vector
    :param tm_ind: 'p' for summation (plus), 't' for multiplication
    RESTRICTION: size of v2, the number of columns, _has_ to be smaller than 64
    """
    m_rows = len(v1)
    m_cols = len(v2)
    type_used = v1.dtype  # v1 and v2 are of the same type
    # if not transpose_ind:
    m_new = gpa.empty((m_rows, m_cols), dtype=type_used)

    if type_used == np.float32:
        vtpv_f = {'t': vtv_f, 'p': vpv_f}
    else:
        vtpv_f = {'t': vtv_double_f, 'p': vpv_double_f}

    rows_to_do = m_rows/1024 + 1  # 1024 ... nb_rows/1024 threads
    block_dims = (1024, 1, 1)
    grid_dims = (65535, 1)
    vtpv_f[tm_ind](v1, v2, m_new, np.int32(m_cols), np.int32(m_rows),
                   np.int32(rows_to_do),
                   block=block_dims, grid=grid_dims)
    return m_new


def vtpv(v1, v2, tm_ind='p', transpose_ind=False):
    """
    vector times/plus vector - constructs a matrix
    :param v1, v2: 2 vectors, first serves as column vector, second as row vector
    :param tm_ind: 'p' for summation (plus), 't' for multiplication
    RESTRICTION: size of v2, the number of columns, _has_ to be smaller than 64
    """
    m_rows = len(v1)
    m_cols = len(v2)
    type_used = v1.dtype  # v1 and v2 are of the same type
    # if not transpose_ind:
    m_new = gpa.empty((m_rows, m_cols), dtype=type_used)
    # else:
    #    m_new = gpa.empty((m_cols, m_rows), dtype=np.float32)

    block_dims = (m_cols, 1, 1)  # THIS HAS TO BE m_cols, 1, 1 & m_cols < 64
    if type_used == np.float32:
        vtpv_f = {'t': vtv_f, 'p': vpv_f}
    else:
        if m_cols < 64:
            vtpv_f = {'t': vtv_double_f, 'p': vpv_double_f}
        else:
            vtpv_f = {'t': vtv_double_f_slow, 'p': vpv_double_f_slow}

    if m_rows / 65535 > 0:
        nb_launches = m_rows / 65535 + 1  # this is an integer
        grid_dims = (65535, 1)
        vtpv_f[tm_ind](v1, v2, m_new, np.int32(m_cols), np.int32(m_rows),
                       np.int32(nb_launches),
                       block=block_dims, grid=grid_dims)
    else:
        grid_dims = (m_rows, 1)
        vtpv_f[tm_ind](v1, v2, m_new, np.int32(m_cols), np.int32(m_rows),
                       np.int32(1),
                       block=block_dims, grid=grid_dims)
    return m_new


def vtpv_new(v1, v2, tm_ind='p'):
    """
    vector times/plus vector - constructs a matrix
    :param v1, v2: 2 vectors, first serves as column vector, second as row vector
    :param tm_ind: 'p' for summation (plus), 't' for multiplication
    """
    m_cols = len(v2)
    m_rows = len(v1)
    type_used = v1.dtype
    # if not transpose_ind:
    m_new = gpa.empty((m_rows, m_cols), dtype=type_used)
    if tm_ind == 'p':
        for row_idx, v1_elt in enumerate(v1):
            m_new[row_idx, :] = np.float32(np.array(v1_elt.get())) + v2
    else:
        for row_idx, v1_elt in enumerate(v1):
            m_new[row_idx, :] = v2 * np.float32(np.array(v1_elt.get()))

    return m_new


def set_mat_by_vec(v, nb_cols):
    """
    set the matrix by column vector 
    only useful if number of rows (len of v) is much smaller than nb_cols
    """
    m_new = gpa.empty((len(v), nb_cols), dtype=np.double)
    for row_nb in range(len(v)):
        gpu_set_const_double_k(m_new[row_nb, :], v[row_nb])

    return m_new


def amax_gpu_0(m):
    """
    computes the amax of a matrix along 0-th axis 
    """
    rows, cols = m.shape
    curr_max = m[0, :]
    for row_nb in range(1, rows):
        curr_max = gpa.maximum(curr_max, m[row_nb, :])

    return curr_max
    

# vector + matrix slicing kernel - vpm
# vector * matrix slicing kernel - vtm
# TO CORRECT: N_STEP IS FIXED. 
vtpm_code = open(os.path.join(config.work_dir, 'cuda', 'vtpm.c'), 'r').read()
# same as above, except that the multiplication is on cols
vtpm_cols_code = open(os.path.join(config.work_dir, 'cuda', 'vtpm_cols.c'), 'r').read()
# vector + matrix function on rows 
vtpm_module = SourceModule(vtpm_code)
vpm_f = vtpm_module.get_function("vpm")
vtm_f = vtpm_module.get_function("vtm")
vpm_double_f = vtpm_module.get_function("vpm_double")
vtm_double_f = vtpm_module.get_function("vtm_double")
# vector + matrix function on columns 
vtpm_cols_module = SourceModule(vtpm_cols_code)
vpm_cols_f = vtpm_cols_module.get_function("vpm_cols")
vtm_cols_f = vtpm_cols_module.get_function("vtm_cols")
vpm_cols2_f = vtpm_cols_module.get_function("vpm_cols2")
vtm_cols2_f = vtpm_cols_module.get_function("vtm_cols2")
# double arithmetics
vpm_cols_double_f = vtpm_cols_module.get_function("vpm_cols_double")
vtm_cols_double_f = vtpm_cols_module.get_function("vtm_cols_double")
vpm_cols2_double_f = vtpm_cols_module.get_function("vpm_cols2_double")
vtm_cols2_double_f = vtpm_cols_module.get_function("vtm_cols2_double")


def vtpm(v, m, tm_ind='p', new_mtx_gen=False):
    """ 
    vector times matrix, by rows 
    v ... vector
    m ... matrix 
    tm_ind = p ... for summation (plus)
    tm_ind = t ... for multiplication (times)
    new_mtx_gen ... indicator whether to generate a new matrix 
    """
    m_cols = m.shape[1]
    m_rows = m.shape[0]
    type_used = v.dtype
    nb_launches = m_rows / 65535 + 1  # this is an integer

    if new_mtx_gen:
        m_new = m.copy()  # copies data
    else:
        m_new = m

    block_dims = (m_cols, 1, 1)
    if type_used == np.float32:
        vtpm_f = {'t': vtm_f, 'p': vpm_f}  # for a single launch this works best
    else:  # double type
        vtpm_f = {'t': vtm_double_f, 'p': vpm_double_f}  # for a single launch this works best

    if m_rows / 65535 > 0:
        grid_dims = (65535, 1)
        vtpm_f[tm_ind](v, m_new, np.int32(m_cols), np.int32(m_rows), 
                       np.int32(nb_launches), 
                       block=block_dims, grid=grid_dims)
    else:
        grid_dims = (m_rows, 1)
        vtpm_f[tm_ind](v, m_new, np.int32(m_cols), np.int32(m_rows), np.int32(1),
                       block=block_dims, grid=grid_dims)
    
    if new_mtx_gen:
        return m_new


def vtpm_cols(v, m, tm_ind='p', cons_new=False):
    """ 
    vector times matrix, by columns:
    _only_ works fast if there are lots of cols, and few rows.
    :param cons_new: if False: m = v */+ m, else m_new = v */+ m
    :param v: vector on host
    :param m: matrix on the device
    :param tm_ind = p ... for summation (plus)
           tm_ind = t ... for multiplication (times)
    """
    m_cols = m.shape[1]
    m_rows = m.shape[0]
    type_used = m.dtype
    # nb_launches = m_rows / 65535 +1 # this is an integer

    # grid_dims = (m_cols / 512 + 1, m_rows)
    grid_dims = (m_cols / 512 + 1, 1)
    block_dims = (512, 1, 1)
    if type_used == np.float32:
        vtpm_cols_f = {'t': vtm_cols2_f, 'p': vpm_cols2_f}  # for a single launch this works best
        for i in range(m_rows):
            vtpm_cols_f[tm_ind](np.float32(v[i]), m, np.int32(m_cols), np.int32(i),
                                block=block_dims, grid=grid_dims)
    else:
        vtpm_cols_f = {'t': vtm_cols2_double_f, 'p': vpm_cols2_double_f}
        for i in range(m_rows):
            vtpm_cols_f[tm_ind](v[i], m, np.int32(m_cols), np.int32(i),
                                block=block_dims, grid=grid_dims)


vtpm_cols_elementwise = ElementwiseKernel("float *v, float *m, float *m_new",
                                          "m_new[i,:] = v[i] + m[i,:]",
                                          "vtpm_cols_used")


def vtpm_cols_new(v, m, tm_ind='p'):
    """
    vector times matrix, by columns: m_new = v */+ m
    :param v: vector on device
    :param m: matrix on the device
    :param tm_ind = p ... for summation (plus)
           tm_ind = t ... for multiplication (times)
    """
    m_rows = m.shape[0]
    m_new = gpa.empty_like(m)

    if tm_ind == 'p':
        for i in range(m_rows):
            m_new[i, :] = np.float32(v[i].get()) + m[i, :]  # WRONG WRONG WRONG SLOW SLOW LSOW
    else:
        for i in range(m_rows):
            m_new[i, :] = np.float32(v[i].get()) * m[i, :]  # WRONG WRONG WRONG

    return m_new


def vtpm_rows_new_ao(v, m, tm_ind='p'):
    """
    vector times matrix, by columns: m_new = v */+ m
    :param v: vector on device
    :param m: matrix on the device
    :param tm_ind = p ... for summation (plus)
           tm_ind = t ... for multiplication (times)
    """
    m_rows = m.shape[0]
    m_new = gpa.empty_like(m)

    if tm_ind == 'p':
        for i in range(m_rows):
            # m_new[i, :] = v + m[i, :]
            m[i, :] += v
    else:
        for i in range(m_rows):
            m[i, :] *= v

    # return m_new


def vtpm_cols_new_hd(v, m, tm_ind='p'):
    """
    vector times matrix, by columns: m_new = v */+ m
    :param v: vector on host
    :param m: matrix on the device
    :param tm_ind = p ... for summation (plus)
           tm_ind = t ... for multiplication (times)
    """
    m_rows = m.shape[0]
    m_new = gpa.empty_like(m)

    if tm_ind == 'p':
        for i in range(m_rows):
            m_new[i, :] = v[i] + m[i, :]
    else:
        for i in range(m_rows):
            m_new[i, :] = v[i] * m[i, :]

    return m_new


def vtpm_cols_new_hd_ao(v_plus, v_mult, m):
    """
    performs v_plus + v_mult * m, where v_plus, v_mult are column vectors 
    :param m: matrix on the device
    """
    m_rows, m_cols = m.shape
    # m_new = gpa.empty_like(m)
    h = cublas.cublasCreate()
    
    for i in range(m_rows):
        # m_new[i, :] = v_plus[i] + v_mult[i] * m[i, :]
        linear_fct_vec(v_mult[i], v_plus[i], m[i, :])
        # cublas.cublasDaxpy(h, m_cols, v_mult[i], m[i, :].gpudata, 1, v_plus[i], 0)

    # return m_new
    return m


# computes a**v, where a is a number, v is a vector 
# --- became part of gpuarray - apow function 
# vpow_module = SourceModule(vpow_code)
# vpow_f = vpow_module.get_function ("vpow") # vector + matrix function
# vpow = lambda a,v: vpow_f(a,v, block=(1,1,1), grid=(len(v),1))
cumsum_cuda_code = open(os.path.join(config.work_dir, 'cuda', 'cumsum_cuda.c'), 'r').read()
cumsum_module = SourceModule(cumsum_cuda_code)
cumsum_cuda_f = cumsum_module.get_function("cumsum_cuda")


def cumsum_cuda(m_d):
    """
    row cumsum function (on cuda, m is a matrix), replaces m_d with
    a matrix of cumsums
    """
    m_d_sh_len = len(m_d.shape)
    if m_d_sh_len == 2:
        m_cols = m_d.shape[1]
        m_rows = m_d.shape[0]
    else:
        m_rows = 1
        m_cols = m_d.shape[0]

    nb_launches = m_rows / 65535 + 1  # number of rows that each thread does

    block_dims = (1, 1, 1)
    grid_dims = (65535, 1)  # rows
    cumsum_cuda_f(m_d, np.int32(m_cols), np.int32(m_rows), np.int32(nb_launches),
                  block=block_dims, grid=grid_dims)

    return 0



with open(os.path.join(config.work_dir, 'cuda', 'rowsum_cuda.c'), 'r') as rowsum_cuda:
    rs_module = SourceModule(rowsum_cuda.read())
    rs_cuda_f = rs_module.get_function("rowsum_cuda")
    rs_cuda_d = rs_module.get_function("rowsum_cuda_double")

with open(os.path.join(config.work_dir, 'cuda', 'colsum_cuda.c'), 'r') as colsum_cuda:
    cs_module = SourceModule(colsum_cuda.read())
    cs_cuda_f = cs_module.get_function("colsum_cuda")
    cs_cuda_last_f = cs_module.get_function("colsum_cuda_last")
    cs_double_cuda_f = cs_module.get_function("colsum_double_cuda")
    cs_double_cuda_last_f = cs_module.get_function("colsum_double_cuda_last")


def colsum_cuda(m_d):
    """ Equivalent to np.cumsum (m_d, axis = 0). Works fast if m_d.cols is large and m_d.rows is small

    :param m_d:
    """

    m_cols = m_d.shape[1]
    m_rows = m_d.shape[0]
    type_used = m_d.dtype
    # nb_launches = m_rows / 65535 +1 # this is an integer

    block_dims = (512, 1, 1)
    grid_dims = (m_cols/512 + 1, 1) 
    cs_f = cs_cuda_f if type_used == np.float32 else cs_double_cuda_f

    for i in range(1, m_rows):
            cs_f(m_d, np.int32(m_cols), np.int32(m_rows), np.int32(i), block=block_dims, grid=grid_dims)


def colsum_cuda_last(m_d):
    """
    equivalent to np.sum (m_d, axis = 0)
    never works fast, although THIS IS STRANGE 
    works "faster" if m_d.cols is large and m_d.rows is small 
    """
    m_cols = m_d.shape[1]
    m_rows = m_d.shape[0]
    type_used = m_d.dtype
    res_d = gpa.zeros(m_cols, dtype=type_used)

    block_dims = (512, 1, 1)
    grid_dims = (m_cols/512 + 1, 1) 
    if type_used == np.float32:
        cs_f = cs_cuda_last_f
    else:
        cs_f = cs_double_cuda_f

    for i in range(m_rows):
        cs_f(m_d, res_d, np.int32(m_cols), np.int32(m_rows), np.int32(i),
             block=block_dims, grid=grid_dims)

    return res_d


def rowsum_cuda(m_d, ones_d, rs_res_d):
    """
    row sum cuda - same as cumsum cuda, just that it returns last col of the matrix, 
    a final cummulative sum 
    """
    # m_cols = m_d.shape[1]
    # m_rows = m_d.shape[0]
    # ones_d = gpa.to_gpu ( np.ones ((m_cols,1)).astype(np.float32) )
    cublas.cublasSgemv_d(1., m_d, ones_d, 0., rs_res_d)


def rowsum_cuda_backup(m_d):
    rs_res_d = gpa.empty(m_d.shape[0], dtype=m_d.dtype)
    m_cols = m_d.shape[1]
    m_rows = m_d.shape[0]
    nb_launches = m_rows / 65535 + 1  # this is an integer

    block_dims = (1, 1, 1)
    grid_dims = (65535, 1)  # rows

    if m_d.dtype == np.float32:
        rs_used = rs_cuda_f
    else:
        rs_used = rs_cuda_d

    rs_used(m_d, rs_res_d, np.int32(m_cols), np.int32(m_rows),
            np.int32(nb_launches),
            block=block_dims, grid=grid_dims)
    return rs_res_d


def rowsum_cuda_notransfer(m_d, rs_res_d):
    """  Sums of rows of the matrix m_d are written in v_d, which is already a device vector.

    """

    # size rs_res_d == m_rows

    m_cols = m_d.shape[1]
    m_rows = m_d.shape[0]
    nb_launches = m_rows / 65535 + 1  # this is an integer

    block_dims = (1, 1, 1)
    grid_dims = (65535, 1)  # rows
    rs_cuda_f(m_d, rs_res_d, np.int32(m_cols), np.int32(m_rows), np.int32(nb_launches), 
              block=block_dims, grid=grid_dims)

    return rs_res_d


# BACKUP COPY, DO NOT TOUCH
def rowsum_cuda_notransfer_backup(m_d, rs_res_d):
    """ Sums of rows of the matrix m_d are written in v_d, which is already a device vector.
    """
    # size rs_res_d == m_rows

    m_cols = m_d.shape[1]
    m_rows = m_d.shape[0]
    nb_launches = m_rows / 65535 + 1  # this is an integer

    block_dims = (1, 1, 1)
    grid_dims = (65535, 1)  # rows
    rs_cuda_f(m_d, rs_res_d, np.int32(m_cols), np.int32(m_rows), np.int32(nb_launches), 
              block=block_dims, grid=grid_dims)
    return rs_res_d


# writes the vector v in col n of matrix m 
# nb_sims is the number of rows (simulations in rows)
# nb_cols ... number of columns 
write_vec_in_mat_col_code = open(os.path.join(config.work_dir, 'cuda', 'write_vec_in_mat.c'), 'r').read()
wohdd_module = SourceModule(write_vec_in_mat_col_code)
wohdd_f = wohdd_module.get_function("write_vec_in_mat_col")


def write_vec_in_mat_col(rowsum_vec_d, hdd_sim_d, n):
    """
    implements the following: hdd_sim[n,:] = rowsum_vec_d
    """
    nb_sims = hdd_sim_d.shape[0]  # nb. rows, simulations in rows
    nb_days = hdd_sim_d.shape[1]  # nb. cols, days are in cols
    wohdd_f(rowsum_vec_d, hdd_sim_d, np.int32(n), np.int32(nb_sims), np.int32(nb_days),
            block=(nb_sims / 65535 + 1, 1, 1), grid=(65535, 1))


# matrix multiplication
matmul_code = open(os.path.join(config.work_dir, 'cuda', 'matmul.c'), 'r').read()
matmul_mod = SourceModule(matmul_code)
matmul_cuda = matmul_mod.get_function("matrixMultiply")
matmul_double_cuda = matmul_mod.get_function("matrixMultiply_double")


def matmul(a_gpu, b_gpu, c_gpu,
           block_size=16):
    """
    computes C_d = A_d(nxm) * B_d(mxk) (for matrix multiplication)
    """
    # set grid size
    type_used = a_gpu.dtype
    m, n = a_gpu.shape
    n_irr, k = b_gpu.shape
    mi, ni, ki = np.int32(m), np.int32(n), np.int32(k)
    grid = ((k-1)/block_size+1, (m-1)/block_size+1, 1)
    # call gpu function
    if type_used == np.float32:
        mm_f = matmul_cuda
    else:
        mm_f = matmul_double_cuda

    mm_f(a_gpu, b_gpu, c_gpu,
         mi, ni, ni, ki, mi, ki,
         block=(block_size, block_size, 1),
         grid=grid)


def matmul_new(a_gpu, b_gpu,
               block_size=16):
    """
    computes C_d = A_d(nxm) * B_d(mxk) (for matrix multiplication) and creates a new matrix 
    """
    # set grid size
    c_gpu = gpa.empty((a_gpu.shape[0], b_gpu.shape[1]), dtype=np.float32)
    matmul(a_gpu, b_gpu, c_gpu, block_size=block_size)
    return c_gpu


gpu_set_const_float_k = ElementwiseKernel('float *m_new, float a',
                                          'm_new[i] = a;',
                                          'gpu_set_const_float_k')


gpu_set_const_double_k = ElementwiseKernel('double *m_new, double a',
                                           'm_new[i] = a;',
                                           'gpu_set_const_double_k')


def gpu_set_constant(m_size, a, dtype=np.double):
    m_new = gpa.empty(m_size, dtype=dtype)
    if dtype == np.float32:
        gpu_set_const_float_k(m_new, a)
    else:
        gpu_set_const_double_k(m_new, a)

    return m_new


def gpu_set_constant_integer(m_d, a):
    """
    sets the number a into an integer array m_d
    IMPORTANT: m_d has to be initialized as dtype=np.int32
    """
    gpa.drv.memset_d32(np.longlong(m_d.ptr), a, m_d.size)


def gpu_set_const_int_mtx(nb_elts, a):
    """
    sets the number a into an integer array m_d
    IMPORTANT: m_d has to be initialized as dtype=np.int32
    """
    m_d = gpa.empty(nb_elts, dtype=np.int32)
    gpa.drv.memset_d32(np.longlong(m_d.ptr), a, m_d.size)
    return m_d


min_int_two = ElementwiseKernel("int *a, int *b, int *c",
                                "c[i] = min(a[i], b[i])",
                                "min_int_two")
max_int_two = ElementwiseKernel("int *a, int *b, int *c",
                                "c[i] = max(a[i], b[i])",
                                "max_int_two")

min_int_three = ElementwiseKernel("int *a, int *b, int *c, int *d",
                                  "d[i] = min(min(a[i], b[i]), c[i])",
                                  "min_int_three")
max_int_three = ElementwiseKernel("int *a, int *b, int *c, int *d",
                                  "d[i] = max(max(a[i], b[i]), c[i])",
                                  "max_int_three")


# wrapper functions
def min_int_two_cons(a, b):
    c = gpa.empty(a.size, dtype=np.int32)
    min_int_two(a, b, c)
    return c


def max_int_two_cons(a, b):
    c = gpa.empty(a.size, dtype=np.int32)
    max_int_two(a, b, c)
    return c


def min_int_three_cons(a, b, c):
    d = gpa.empty(a.size, dtype=np.int32)
    min_int_three(a, b, c, d)
    return d


def max_int_three_cons(a, b, c):
    d = gpa.empty(a.size, dtype=np.int32)
    max_int_three(a, b, c, d)
    return d


# negate a bool array
negate_bool_k = ElementwiseKernel("bool *a, bool *b",
                                  "b[i] = !a[i]",
                                  "negate_bool")


def negate_bool(a):
    """
    negates a bool vector
    """
    b = gpa.empty(a.size, dtype=bool)
    negate_bool_k(a, b)
    return b


# comparison operators 
comp_array_float_smaller_k = ElementwiseKernel("float *a, float b, bool *c", 
                                               "c[i] = a[i] <= b;", 
                                               name="comp_array_float_smaller_k")

comp_array_int_smaller_k = ElementwiseKernel("int *a, float b, bool *c", 
                                             "c[i] = a[i] <= b;", 
                                             name="comp_array_int_smaller_k")

comp_array_float_larger_k = ElementwiseKernel("float *a, float b, bool *c", 
                                              "c[i] = a[i] > b;",
                                              name="comp_array_float_larger_k")

comp_array_int_larger_k = ElementwiseKernel("int *a, float b, bool *c", 
                                            "c[i] = a[i] > b;",
                                            name="comp_array_int_larger_k")

and_array_k = ElementwiseKernel("bool *a, bool *b, bool *c", 
                                "c[i] = a[i] & b[i];", 
                                name="and_array_k")

or_array_k = ElementwiseKernel("bool *a, bool *b, bool *c", 
                               "c[i] = a[i] | b[i];", 
                               name="or_array_k")


def comp_array_number(a, b, op='smaller', dtype='float'):
    c = gpa.empty(a.size, dtype=bool)
    if dtype == 'float':
        if op == 'smaller':
            comp_array_float_smaller_k(a, b, c)
        else:
            comp_array_float_larger_k(a, b, c)
    else:
        if op == 'smaller':
            comp_array_int_smaller_k(a, b, c)
        else:
            comp_array_int_larger_k(a, b, c)

    return c


def and_or_array(a, b, op='and'):
    c = gpa.empty(a.size, dtype=bool)
    if op == 'and':
        and_array_k(a, b, c)
    else:
        or_array_k(a, b, c)

    return c


comp_two_arrays_and_k = ElementwiseKernel("int *a1, int *a2, float b1, float b2, bool *res",
                                          "res[i] = (a1[i] < b1) & (a2[i] > b2)",
                                          name="comp_two_arrays_and_k")


def comp_two_arrays_and(a1, a2, b1, b2):
    """
    usage in tolling_cmg startup decision
    """
    c = gpa.empty(a1.size, dtype=bool)
    comp_two_arrays_and_k(a1, a2, b1, b2, c)
    return c


take_part_array_k = ElementwiseKernel("float *b, float *a, int st_idx",
                                      "b[i] = a[i+st_idx];",
                                      name="take_part_array_k")


# multiply vector with float 
mv = ElementwiseKernel("float *a, float *b, float c",
                       "b[i] = c * a[i];",
                       name='multiply_vec')


def mult_vec(a, b):
    c = gpa.empty(a.size, dtype=np.float32)
    mv(a, c, b)
    return c


# broadcasting a short vector (sv) onto long vector (lv), 
# used for matrix multiplication 
bdcast_code = open(os.path.join(config.work_dir, 'cuda', 'bdcast.c'), 'r').read()
bdcast_mod = SourceModule(bdcast_code)
bdcast_f = bdcast_mod.get_function('bdcast')


def bdcast(corr_m, rn_v):
    """
    :param corr_m: cholesky decomposition of the correlation matrix, for now 2x2 matrix 
    :param rn_v: random number vector 
    """
    nb_sims = rn_v.shape[1] * rn_v.shape[0]  # number of simulations
    res_v = gpa.empty_like(rn_v)  # this has to be true
    bdcast_f(corr_m, rn_v, res_v, np.int32(corr_m.shape[0]), np.int32(nb_sims),
             block=(512/corr_m.shape[0], corr_m.shape[0], 1), grid=(nb_sims/512*corr_m.shape[0]+1, 1), 
             shared=corr_m.shape[0] * 4)  # shared memory (nb of bytes) 
    return res_v


cdf_k_f = ElementwiseKernel("float *x, float *res",
                           """
                           float a1 = 0.31938153;
                           float a2 = -0.356563782;
                           float a3 = 1.781477937;
                           float a4 = -1.821255978;
                           float a5 = 1.330274429;
                           float L = fabs(x[i]);
                           float K = 1. / (1. + 0.2316419 * L);
                           float K2 = K * K;
                           float K4 = K2 * K2;
                           // 0.39... = 1/(sqrt(2*pi))
                           float w =  1. - 0.3989422804 * expf(-L * L / 2) * (a1 * K + a2 * K2 +
                               a3 * K2 * K + a4 * K4 + a5 * K4 * K);
                           res[i] = w * (x[i] >= 0.) + (1. - w) * (x[i] < 0.);
                           """, name='cdf_k_f')

cdf_k_d = ElementwiseKernel("double *x, double *res",
                            """
                            double a1 = 0.31938153;
                            double a2 = -0.356563782;
                            double a3 = 1.781477937;
                            double a4 = -1.821255978;
                            double a5 = 1.330274429;
                            double L = fabs(x[i]);
                            double K = 1. / (1. + 0.2316419 * L);
                            double K2 = K * K;
                            double K4 = K2 * K2;
                            // 0.39... = 1/(sqrt(2*pi))
                            double w =  1. - 0.3989422804 * exp(-L * L / 2) * (a1 * K + a2 * K2 +
                                a3 * K2 * K + a4 * K4 + a5 * K4 * K);
                            res[i] = w * (x[i] >= 0.) + (1. - w) * (x[i] < 0.);
                            """, name='cdf_k_d')


def cdf_vec_gpu(x):
    res = gpa.empty(x.shape, dtype=x.dtype)
    if x.dtype == np.float32:
        cdf_k_f(x, res)
    else:  # double
        cdf_k_d(x, res)
    return res


# multiply vector with float 
linear_fct_vec = ElementwiseKernel("float s, float d, float *vec",
                                   "vec[i] = s * vec[i] + d;",
                                   name='linear_fct_vec')
