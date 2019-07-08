__global__ void rowsum_cuda(float *M, float *v, 
                            int nb_cols, int nb_rows, int to_do_rows) {

  int ind1, ind2;
  int row_start_idx;
  int curr_row = blockIdx.x * blockDim.x + threadIdx.x;

  /* initial step */ 
  if (curr_row < nb_rows )
    v[curr_row] = M[curr_row]; 

  /* for each column */
  for (ind2 = 1; ind2 < nb_cols; ind2 += 1)
    if ( curr_row < nb_rows ) 
      v[curr_row] +=  M[ind2 * nb_rows + curr_row]; 
}


__global__ void rowsum_cuda_double(double *M, double *v,
				   int nb_cols, int nb_rows, int to_do_rows) {

  int ind1, ind2;
  int row_start_idx;
  int curr_row = blockIdx.x * blockDim.x + threadIdx.x;

  /* initial step */ 
  if (curr_row < nb_rows )
    v[curr_row] = M[curr_row]; 

  /* for each column */
  for (ind2 = 1; ind2 < nb_cols; ind2 += 1)
    if ( curr_row < nb_rows ) 
      v[curr_row] +=  M[ind2 * nb_rows + curr_row]; 
}
