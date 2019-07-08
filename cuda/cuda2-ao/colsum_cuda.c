__global__ void colsum_cuda (float *M, int nb_cols, int nb_rows, int row_idx) {
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  if ( col_idx < nb_cols )
    M[row_idx * nb_cols + col_idx ] += M[(row_idx-1)*nb_cols + col_idx];
}

__global__ void colsum_cuda_last (float *M, float *res, int nb_cols, int nb_rows, int row_idx) {
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  if (col_idx < nb_cols)
    res[ col_idx ] += M[row_idx*nb_cols + col_idx];
}

// double arithmetics
__global__ void colsum_double_cuda(double *M, int nb_cols, int nb_rows, int row_idx) {
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  if (col_idx < nb_cols)
    M[row_idx * nb_cols + col_idx] += M[(row_idx-1)*nb_cols + col_idx];
}

__global__ void colsum_double_cuda_last(double *M, double *res, int nb_cols, int nb_rows, int row_idx) {
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  if (col_idx < nb_cols)
    res[col_idx] += M[row_idx*nb_cols + col_idx];
}
