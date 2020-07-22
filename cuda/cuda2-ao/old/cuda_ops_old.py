
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
