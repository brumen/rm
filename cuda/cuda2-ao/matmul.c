// matrix multiplication CUDA code

__global__ void matrixMultiply( float *A
                                , float *B
                                , float *C
                                , int numARows
                                , int numAColumns
                                , int numBRows
                                , int numBColumns
                                , int numCRows
                                , int numCColumns) {

  //@@ Insert code to implement matrix multiplication here
  __shared__ float ds_M[16][16];
  __shared__ float ds_N[16][16];

  int bx = blockIdx.x,  by = blockIdx.y,
    tx = threadIdx.x, ty = threadIdx.y,
    Row = by * 16 + ty,
    Col = bx * 16 + tx;
  float Pvalue = 0;

  for (int m = 0; m < (numAColumns-1)/16+1; ++m) {

    if (Row < numARows && m*16+tx < numAColumns)
      ds_M[ty][tx] = A[Row*numAColumns + m*16+tx];
    else
      ds_M[ty][tx] = 0;

    if (Col < numBColumns && m*16+ty < numBRows)
      ds_N[ty][tx] = B[(m*16+ty)*numBColumns+Col];
    else
      ds_N[ty][tx] = 0;

    __syncthreads();

    for (int k = 0; k < 16; ++k)
      Pvalue += ds_M[ty][k] * ds_N[k][tx];
    __syncthreads();
  }

  if (Row < numCRows && Col < numCColumns)
    C[Row*numCColumns+Col] = Pvalue;

}

__global__ void matrixMultiply_double( double *A
                                       , double *B
                                       , double *C
                                       , int numARows
                                       , int numAColumns
                                       , int numBRows
                                       , int numBColumns
                                       , int numCRows
                                       , int numCColumns ) {

  //@@ Insert code to implement matrix multiplication here
  __shared__ double ds_M[16][16];
  __shared__ double ds_N[16][16];

  int bx = blockIdx.x,  by = blockIdx.y,
    tx = threadIdx.x, ty = threadIdx.y,
    Row = by * 16 + ty,
    Col = bx * 16 + tx;
  double Pvalue = 0;

  for (int m = 0; m < (numAColumns-1)/16+1; ++m) {

    if (Row < numARows && m*16+tx < numAColumns)
      ds_M[ty][tx] = A[Row*numAColumns + m*16+tx];
    else
      ds_M[ty][tx] = 0;

    if (Col < numBColumns && m*16+ty < numBRows)
      ds_N[ty][tx] = B[(m*16+ty)*numBColumns+Col];
    else
      ds_N[ty][tx] = 0;

    __syncthreads();

    for (int k = 0; k < 16; ++k)
      Pvalue += ds_M[ty][k] * ds_N[k][tx];

    __syncthreads();

  }

  if (Row < numCRows && Col < numCColumns)
    C[Row*numCColumns+Col] = Pvalue;

}
