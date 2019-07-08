/* read from python code */
/* used in tolling.py code */

#define nb_cols %(mat_cols)d


#include <cuda_runtime.h>
#include <cublas_v2.h>

/* 
   replaces the matrix M with cumsum of M 
   tolling.py uses this, SHOULD BE REWRITTEN
*/
__global__ void cumsum_cuda (float *M ) {

  int ind1;
  float curr_val;
  int res_idx = threadIdx.x + blockIdx.x * blockDim.x ;

  curr_val =  M[res_idx * nb_cols]; 
  for (ind1 = 1; ind1 < 30 ; ind1 = ind1 + 1) {
    curr_val = curr_val + M[res_idx * nb_cols + ind1];
    M[res_idx * nb_cols + ind1] = curr_val;
  }
}

/* 
   tolling.py uses this
   kernel that writes the n-th column of M into c, nb_sims is the number of simulations
*/
__global__ void get_col (float *M, int *day, int *nb_sims, float *c ) {

  int res_idx = threadIdx.x + blockIdx.x * blockDim.x + (*day) * (*nb_sims) ;
  
  if ( res_idx < ( (*day) + 1) * (*nb_sims) )
    c[res_idx] = M[res_idx];

}



/* 
   weather.py uses this, not optimally written, but not bad.
   replaces the matrix M with cumsum of M, 
   more general than above
*/
__global__ void cumsum_cuda_new ( float *M ) {

  int col = blockIdx.x * blockDim.x ; /* blockDim.x is the number of columns */
  int ind1;
  __shared__ float sh_row[nb_cols]; /* shared row */

  sh_row[threadIdx.x] = M[col + threadIdx.x]; /* copy to shared mem */
  __syncthreads();

  /* really SLOW way of implementing cumsum */
  if (threadIdx.x == 0) {
    for (ind1 = 1; ind1 < nb_cols; ind1 = ind1 + 1)
      sh_row[ind1] += sh_row[ind1 -1];
  }
  __syncthreads();

  /* read back into M */
  M[col+threadIdx.x] = sh_row[threadIdx.x];
}


/* sums the rows in matrix M */
/* just one thread launches */
__global__ void row_sum_cuda ( float *M, float *res ) {

  int col = blockIdx.x * nb_cols ; /* blockDim.x is the number of columns */
  int ind1;
  float row_sum = 0.0;

  /* really SLOW way of implementing cumsum */
  for (ind1 = 0; ind1 < nb_cols; ind1 = ind1 + 1)
    row_sum += M[col + ind1]; 

  res[blockIdx.x] = row_sum;

}
