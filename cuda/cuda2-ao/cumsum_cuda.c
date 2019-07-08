__global__ void cumsum_cuda(float *M, int nb_cols, int nb_rows, int to_do_rows) {

  int ind1, ind2, curr_row;
  float curr_val;
  int row_start_idx;

  for (ind1 = 0; ind1 < (to_do_rows); ind1 += 1) { /* traversing rows */
    row_start_idx = ind1 * nb_cols * 65535 + blockIdx.x * nb_cols;
    curr_row = ind1 * 65535 + blockIdx.x;
    if (curr_row < nb_rows) {
      curr_val =  M[row_start_idx];
      for (ind2 = 1; ind2 < nb_cols; ind2 += 1) { /* traversing individual row across cols */
        curr_val = curr_val + M[row_start_idx + ind2];
        M[row_start_idx + ind2] = curr_val;
        //M[0] = (float) 2.;
      }
    }
  }
}
