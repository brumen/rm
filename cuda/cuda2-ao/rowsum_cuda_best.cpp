template <unsigned int blockSize>
__device__ void warpReduce(volatile float *sdata, unsigned int tid) {
  if (blockSize >= 64) sdata[tid] += sdata[tid + 32];
  if (blockSize >= 32) sdata[tid] += sdata[tid + 16];
  if (blockSize >= 16) sdata[tid] += sdata[tid + 8];
  if (blockSize >= 8) sdata[tid] += sdata[tid + 4];
  if (blockSize >= 4) sdata[tid] += sdata[tid + 2];
  if (blockSize >= 2) sdata[tid] += sdata[tid + 1];
}

template <unsigned int blockSize>
__global__ void reduce6(float *g_data, float *g_odata, unsigned int n) {
  extern __shared__ float sdata[];
  unsigned int tid = threadIdx.x;
  unsigned int i = blockIdx.x * (blockSize*2) + tid;
  unsigned int gridSize = blockSize * 2 * gridDim.x;
  sdata[tid] = 0.;

  while (i<n) {
    sdata[tid] += g_idata[i] + g_idata[i + blockSize];
    i += gridSize;
  }
  __syncthreads();

  if (blockSize >= 512) {
    if (tid < 256)
      sdata[tid] += sdata[tid + 256];
    __syncthreads();
  }
  if (blockSize >= 256) {
    if (tid < 128)
      sdata[tid] += sdata[tid + 128];
    __syncthreads();
  }
  if (blockSize >= 128) {
    if (tid < 64)
      sdata[tid] += sdata[tid + 64];
    __syncthreads();
  }
  if (tid < 32)
    warpReduce(sdata, tid);
  if (tid == 0)
    g_odata[blockIdx.x] = sdata[0];
}

void row_sum_reduce(float *d_idata, float *d_odata) {
  switch (threads) {
  case 512:
    reduce6<512><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 256:
    reduce6<256><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 128:
    reduce6<128><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 64:
    reduce6<64><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 32:
    reduce6<32><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 16:
    reduce6<16><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 8:
    reduce6<8><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 4:
    reduce6<4><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 2:
    reduce6<2><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  case 1:
    reduce6<1><<<dimGrid, dimBlock, smemSize>>>(d_idata, d_odata); break;
  }
}
