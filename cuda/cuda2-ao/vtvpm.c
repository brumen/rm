// vector times vector - constructing a matrix, first vec is column, second is row
// vector + vector broadcasting into a matrix 
// m is the new matrix, space should be allocated  
// size_shared_array: how many cols do we allow, currently 64, appropriate for tolling
#define SIZE_SHARED_ARRAY 64

__global__ void vpv(float *v1, float *v2, float *m,
		    int nb_cols, int nb_rows, int to_do_rows) {
  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  //__shared__ float v2_cache[SIZE_SHARED_ARRAY];
  float v1_local = v1[blockIdx.x];
  //v2_cache[th_idx] = v2[th_idx];
  //__syncthreads();
  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    res_idx = ind1 * (nb_cols) * 65535 + th_bl_idx;
    if (res_idx < (nb_rows) * (nb_cols))
      //m[res_idx] = v1_local + v2_cache[th_idx];
      m[res_idx] = v1_local + v2[th_idx];
  }
}

__global__ void vtv(float *v1, float *v2, float *m,
		    int nb_cols, int nb_rows, int to_do_rows) {
  int ind1, pos_idx;
  int th_idx = threadIdx.x;
  __shared__ float v2_cache[SIZE_SHARED_ARRAY];
  float v1_local = v1[blockIdx.x];
  v2_cache[th_idx] = v2[th_idx]; // write into the cache 
  __syncthreads();
  
  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    pos_idx = ind1 * (nb_cols) * gridDim.x +  blockIdx.x * blockDim.x + th_idx;
    if (pos_idx < (nb_rows) * (nb_cols))
      m[pos_idx] = v1_local * v2_cache[th_idx];
  }
}

__global__ void vtv_old(float *v1, float *v2, float *m,
			int nb_cols, int nb_rows, int to_do_rows) {
  // slower version of the vtv
  int ind1, col_idx, pos_idx;
  int th_idx = threadIdx.x;
  int row_idx = blockIdx.x * 1024 + th_idx;
  __shared__ float v2_cache[SIZE_SHARED_ARRAY];

  if (th_idx < nb_cols)
    v2_cache[th_idx] = v2[th_idx];
  __syncthreads();

  // for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {  // beg. idx of 
  // row_idx = th_idx * to_do_rows;
  for (col_idx=0; col_idx < nb_cols; col_idx += 1) {
    pos_idx = row_idx * nb_cols + col_idx;
    if ((pos_idx < nb_rows * nb_cols) & (row_idx < nb_rows))
      m[pos_idx] = v1[row_idx] * v2_cache[col_idx];
  }
}


__global__ void vpv_double(double *v1, double *v2, double *m, int nb_cols, int nb_rows, int to_do_rows) {
  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  __shared__ double v2_cache[SIZE_SHARED_ARRAY];
  double v1_local = v1[blockIdx.x];
  v2_cache[th_idx] = v2[th_idx];
  __syncthreads();
  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    res_idx = ind1 * (nb_cols) * 65535 + th_bl_idx;
    if (res_idx < (nb_rows) * (nb_cols))
      m[res_idx] = v1_local + v2_cache[th_idx];
  }
}

__global__ void vpv_double_slow(double *v1, double *v2, double *m,
				int nb_cols, int nb_rows, int to_do_rows) {
  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  //__shared__ double v2_cache[SIZE_SHARED_ARRAY];
  double v1_local = v1[blockIdx.x];
  //v2_cache[th_idx] = v2[th_idx];
  //__syncthreads();
  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    res_idx = ind1 * (nb_cols) * 65535 + th_bl_idx;
    if (res_idx < (nb_rows) * (nb_cols))
      m[res_idx] = v1_local + v2[th_idx];
  }
}


__global__ void vtv_double(double *v1, double *v2, double *m,
			   int nb_cols, int nb_rows, int to_do_rows) {
  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  __shared__ double v2_cache[SIZE_SHARED_ARRAY];
  double v1_local = v1[blockIdx.x];
  v2_cache[th_idx] = v2[th_idx];
  __syncthreads();
  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    res_idx = ind1 * (nb_cols) * 65535 + th_bl_idx;
    if (res_idx < (nb_rows) * (nb_cols))
      m[res_idx] = v1_local * v2_cache[th_idx];
  }
}


// same as vtv above, but without the cached v2 vector 
__global__ void vtv_double_slow(double *v1, double *v2, double *m,
				int nb_cols, int nb_rows, int to_do_rows) {
  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  //__shared__ double v2_cache[SIZE_SHARED_ARRAY];
  double v1_local = v1[blockIdx.x];
  //v2_cache[th_idx] = v2[th_idx];
  //__syncthreads();
  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    res_idx = ind1 * (nb_cols) * 65535 + th_bl_idx;
    if (res_idx < (nb_rows) * (nb_cols))
      m[res_idx] = v1_local * v2[th_idx];
  }
}
