// # writes the vector v in col n of matrix m 
// # nb_sims is the number of rows (simulations in rows)
// # nb_cols ... number of columns 

__global__ void write_vec_in_mat_col (float *v, float *m, int n, int nb_sims, int nb_cols) {
  
  int elt_idx = blockIdx.x * blockDim.x + threadIdx.x;

  if (elt_idx < nb_sims ) 
    m[ n  + elt_idx * nb_cols ] = v[elt_idx];
}

__global__ void write_vec_in_mat_row (float *v, float *m, int n, int nb_sims, int nb_cols) {
  
  int elt_idx = blockIdx.x * blockDim.x + threadIdx.x;

  if (elt_idx < nb_sims ) 
    m[ n  + elt_idx * nb_cols ] = v[elt_idx];
}
