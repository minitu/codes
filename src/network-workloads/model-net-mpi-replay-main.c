/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
#include <mpi.h>

#ifdef USE_ONLINE
#include <abt.h>
#endif

#include "codes/codes-mpi-replay.h"

int main(int argc, char** argv) {
  MPI_Init(&argc,&argv);

#ifdef USE_ONLINE
  ABT_init(argc, argv);
#endif

  modelnet_mpi_replay(MPI_COMM_WORLD, &argc, &argv);

#ifdef USE_ONLINE
  ABT_finalize();
#endif

  int flag;
  MPI_Finalized(&flag);
  if (!flag) MPI_Finalize();

  return 0;
}
