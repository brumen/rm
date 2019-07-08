// vector + matrix slicing kernel - vpm
// vector * matrix slicing kernel - vtm
// TO CORRECT: N_STEP IS FIXED. 

__global__ void vpm( float *v
		   , float *m
		   , int nb_cols
		   , int nb_rows
		   , int to_do_rows) {

  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  __shared__ float v_cache[31];
  v_cache[th_idx] = v[th_idx];

  for (ind1 = 0; ind1 < to_do_rows; ind1 = ind1 + 1) {
    res_idx = ind1 * nb_cols * 65535 + th_bl_idx;
    if (res_idx < nb_rows * nb_cols)
      m[res_idx] += v_cache[th_idx];
  }

}

__global__ void vtm( float *v
		   , float *m
		   , int nb_cols
		   , int nb_rows
		   , int to_do_rows) {

  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  __shared__ float v_cache[31];
  v_cache[th_idx] = v[th_idx];

  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    res_idx = ind1 * (nb_cols) * 65535 + th_bl_idx;
    if ( res_idx < (nb_rows) * (nb_cols) )
      m[res_idx] *= v_cache[th_idx];
  }
  
}

__global__ void vpm_double( double *v
			  , double *m
			  , int nb_cols
			  , int nb_rows
			  , int to_do_rows) {
  /* 
     Vector + Matrix for double arrays. TODO: BY ROWS OR COLUMNS 
  */
  
  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  __shared__ double v_cache[31];
  v_cache[th_idx] = v[th_idx];

  for (ind1 = 0; ind1 < to_do_rows; ind1 = ind1 + 1) {
    res_idx = ind1 * nb_cols * 65535 + th_bl_idx;
    if (res_idx < nb_rows * nb_cols)
      m[res_idx] += v_cache[th_idx];
  }
  
}

__global__ void vtm_double( double *v
			  , double *m
			  , int nb_cols
			  , int nb_rows
			  , int to_do_rows) {

  int ind1, res_idx;
  int th_idx = threadIdx.x;
  int th_bl_idx = th_idx + blockIdx.x * blockDim.x;
  __shared__ double v_cache[31];
  v_cache[th_idx] = v[th_idx];

  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) {
    res_idx = ind1 * (nb_cols) * 65535 + th_bl_idx;
    if ( res_idx < (nb_rows) * (nb_cols) )
      m[res_idx] *= v_cache[th_idx];
  }

}
