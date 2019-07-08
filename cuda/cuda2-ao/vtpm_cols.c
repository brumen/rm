// similar to vtpm, except that the multiplication/addition is done on cols
// vector + matrix slicing kernel - vpm
// vector * matrix slicing kernel - vtm
// TO CORRECT: N_STEP IS FIXED. 

__global__ void vpm_cols(float *v, float *m, int nb_cols ) {

  int row_idx = blockIdx.y; 
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;

  float v_curr = v[row_idx];

  //if (threadIdx.x == 0)
  //  v_curr = v[row_idx]; 

  if ( col_idx < nb_cols )
    m[row_idx * nb_cols + col_idx ] += v_curr;

}

__global__ void vtm_cols(float *v, float *m, int nb_cols) {

  int row_idx = blockIdx.y; 
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;

  __shared__ float v_curr;

  if (threadIdx.x == 0)
    v_curr = v[row_idx];
 
  //if (threadIdx.x == 0)
  //  v_curr = v[row_idx]; 

  if ( col_idx < nb_cols )
    m[row_idx * nb_cols + col_idx ] *= v_curr;

}

__global__ void vtm_cols2(float v, float *m, int nb_cols, int row_idx ) {

  //int row_idx = blockIdx.y; 
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;

  //float v_curr = v[row_idx]; 
  
  //if (threadIdx.x == 0)
  //  v_curr = v[row_idx]; 

  if ( col_idx < nb_cols )
    m[row_idx * nb_cols + col_idx ] *= v;

}

__global__ void vpm_cols2(float v, float *m, int nb_cols, int row_idx ) {

  //int row_idx = blockIdx.y; 
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;

  //float v_curr = v[row_idx]; 
  
  //if (threadIdx.x == 0)
  //  v_curr = v[row_idx]; 

  if ( col_idx < nb_cols )
    m[row_idx * nb_cols + col_idx ] += v;

}

// functions related to DOUBLE arithmetics
__global__ void vpm_cols_double(double *v, double *m, int nb_cols) {

  int row_idx = blockIdx.y;
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  double v_curr = v[row_idx];

  if (col_idx < nb_cols)
    m[row_idx * nb_cols + col_idx] += v_curr;
}

__global__ void vtm_cols_double(double *v, double *m, int nb_cols) {

  int row_idx = blockIdx.y;
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  __shared__ double v_curr;

  if (threadIdx.x == 0)
    v_curr = v[row_idx];

  if (col_idx < nb_cols)
    m[row_idx * nb_cols + col_idx] *= v_curr;
}

__global__ void vtm_cols2_double(double v, double *m, int nb_cols, int row_idx) {
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  if (col_idx < nb_cols)
    m[row_idx * nb_cols + col_idx] *= v;
}

__global__ void vpm_cols2_double(double v, double *m, int nb_cols, int row_idx) {
  int col_idx = threadIdx.x + blockIdx.x * blockDim.x;
  if (col_idx < nb_cols)
    m[row_idx * nb_cols + col_idx] += v;
}
