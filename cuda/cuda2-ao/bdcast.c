// broadcasting 
__global__ void bdcast(float *sv, float *lv, float *res,
  		       int sv_nb, int lv_nb) {
  // block size = (512/sv_nb, sv_nb, 1)
  // sv_nb: size of sv matrix (sv_nb x sv_nb)
  // extern __shared__ float sv_sh[];  
  extern __shared__ float lv_sh[];
  int bx = blockIdx.x, tx = threadIdx.x, ty = threadIdx.y;
  size_t ty_idx, take_idx, loop_idx; 
  float sum_curr = 0.;
  take_idx = bx * blockDim.x * blockDim.y + tx* blockDim.y; // + ty;
  ty_idx = blockDim.y * ty;

  lv_sh[ty] = lv[take_idx + ty];
  __syncthreads();

  if(take_idx + sv_nb - 1 < lv_nb) {
    for (loop_idx = 0; loop_idx < sv_nb; loop_idx += 1)
      sum_curr += sv[ty_idx + loop_idx] * lv_sh[loop_idx];
    res[take_idx + ty] = sum_curr;
  }
}
