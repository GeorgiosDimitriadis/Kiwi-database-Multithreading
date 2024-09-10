#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#define KSIZE (16)
#define VSIZE (1000)

#define LINE "+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1 "---------------------------------------------------------------------------------------------------\n"
//ftiaxoume ena kainourgio thread panw sto arxeio h wste na mas dieukolinei kai na einai pio katharos o kwdikas.
typedef struct {
    int r;
    int tnumber;					 
	int count;						
	int threads;
}args;

long long get_ustime_sec(void);
void _random_key(char *key,int length);