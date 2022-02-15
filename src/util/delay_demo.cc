#include "unistd.h"
#include "stdio.h"
#include "stdlib.h"

int main(int argc,char *argv[]){
  
  int delay_sec = ::atoi(argv[1]);
  sleep(delay_sec);
  fprintf(stdout,"stdout: delay %d\n",delay_sec);
  fprintf(stderr,"stderr: delay %d\n",delay_sec);
  return 0;
}
