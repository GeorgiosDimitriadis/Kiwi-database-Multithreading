#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"
#include <pthread.h> 
#define DATAS ("testdb")
DB* db_for_threads; 		//To vazoume ws global gia na mporei na kanei db_open kai db_close apo ola ta nhmata kai synarthseis
void _write_test(long int count, int r)
{
	int i;
	double cost;
	long long start,end;
	Variant sk, sv;
	DB* db;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	db = db_open(DATAS);

	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _read_test(long int count, int r)
{
	int i;
	int ret;
	int found = 0;
	double cost;
	long long start,end;
	Variant sk;
	Variant sv;
	DB* db;
	char key[KSIZE + 1];

	db = db_open(DATAS);
	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		ret = db_get(db, &sk, &sv);
		if (ret) {
			//db_free_data(sv.mem);
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}

		
		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
} 
void *reader(void *arg)
{
    int i;
    int ret;
    long found = 0;
    char key[KSIZE + 1];
    Variant sk, sv;
    args *bench_struct = (args *)arg;//apo edw kai katw kanoume anathesh twn timwn ths domhs stis antistoixes metavlhtes kai orizoume first key last key kai thread keys pou tha mas xreiastoun parakatw
    int count = bench_struct->count;
    int tnumber = bench_struct->tnumber;
    int threads = bench_struct->threads;
    int r = bench_struct->r;
    int thread_keys = count / threads;
    int first_key = tnumber * thread_keys;
    int last_key = first_key + thread_keys - 1;
	if (tnumber == threads - 1) {
        last_key = count - 1;
    }
    for (i = first_key; i <= last_key; i++)
    {
    	memset(key, 0, KSIZE + 1);
        if (r) {
            snprintf(key, KSIZE, "key-%d", rand() % count);
        } else {
            snprintf(key, KSIZE, "key-%d", i);
        }
        sk.length = KSIZE;
        sk.mem = key;
        ret = db_get(db_for_threads, &sk, &sv); //edw pername thn global domh mas
        if (ret)
        {
            found++;
        }
        else
        {
            INFO("not found key#%s", sk.mem);
        }
		if ((i % 1000000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
    }

    printf("%d Thread with id %lu finished and found %ld keys\n\n\n", tnumber, pthread_self(), found); //typwnoume ton arithmo twn kleidiwn pou vrethike gia kathe thread mazi me ta id tous sto struct kai sto systhma
    return (void*)found;
}
void *writer(void *arg)
{
    int i;
    char key[KSIZE + 1];
    char val[VSIZE + 1];
    Variant sk, sv;
    args *bench_struct = (args*)arg;//Idia diadikasia opws sto reader apo panw
    int r = bench_struct->r;
    int count =bench_struct->count;
    int threads = bench_struct->threads;
    int tnumber = bench_struct->tnumber;
    int thread_keys = count / threads;
    int first_key = tnumber * thread_keys;
    int last_key = first_key + thread_keys - 1;
    if (tnumber == threads - 1) {
        last_key = count - 1;
    }
    long int total_writes_thread = 0;

    for (i = first_key; i <= last_key; i++)
    {

        memset(key, 0, KSIZE + 1);
        memset(val, 0, VSIZE + 1);
        if (r) {
            _random_key(key, KSIZE);
            snprintf(val, VSIZE, "val-%d", rand() % count);
        } else {
            snprintf(key, KSIZE, "key-%d", i);
            snprintf(val, VSIZE, "val-%d", i);
        }
        sk.length = KSIZE;
        sk.mem = key;
        sv.length = VSIZE;
        sv.mem = val;
        db_add(db_for_threads, &sk, &sv); // opws kai panw pername to global db

		total_writes_thread+= 1;
    }

	if ((i % 100000) == 0) { 
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}

    printf("%d Thread with id %lu finished and wrote %ld keys\n\n\n", tnumber, pthread_self(), total_writes_thread);
    return NULL;
}

void _readwrite(int count, int r, int threads, int perc)
{
     int found;

    int i;
    double cost;
    long long start, end;
    pthread_t write_threads[100];
    pthread_t read_threads[100];
    int write_thread = threads * perc / 100; //apothikevoume to pososto twn nhmatwn gia write
    int read_thread = threads-write_thread;// apothikevoume to pososto twn nhmatwn gia read me mia aplh afairesh

    db_for_threads = db_open(DATAS);//Open thn global
    start = get_ustime_sec();

    for (i = 0; i < write_thread; i++)
    {
        args *bench_struct = malloc(sizeof(args));//ftiaxnoyme ena neo struct args me malloc kai antistoixoume tis times tou 
	bench_struct->count = ((count * perc) / 100);        
	bench_struct->r = r;        
        bench_struct->threads = write_thread;
	bench_struct->tnumber = i;        
        pthread_create(&write_threads[i], NULL,writer, (void *)bench_struct);//ftiaxoume ta threads kai pername ta orismata katallhla
    }
    for (i = 0; i < read_thread; i++)
    {
        args *bench_struct = malloc(sizeof(args));//ksana h idia diadikasia me panw...
        bench_struct->r = r;
        bench_struct->count = ((count * (100 - perc)) / 100); 
        bench_struct->threads = read_thread; 
	bench_struct->tnumber = i;
        pthread_create(&read_threads[i], NULL, reader , (void *)bench_struct);
    }

    for (i = 0; i < write_thread; i++)//kanoume join gia na perimenoun thn oloklhrwsh olwn twn allwn nhmatwn
    {
        pthread_join(write_threads[i], NULL);
    }
    for (i = 0; i < read_thread; i++)
    {
	void*result;        
	pthread_join(read_threads[i],&result);//edw vazoume result etsi wste na metrame sto found ta kleidia pou vrethikan
	found += (int)result;
        
    }

    db_close(db_for_threads);

    end = get_ustime_sec();
    cost = end - start;
    printf(LINE);
    printf("|Threaded-ReadWrite (done:%d): %.6f (found:%d) sec/op; %.1f ops/sec(estimated); cost:%.3f(sec);\n"
        , count, (double)(cost / count * threads)
        ,found, (double)(count * threads / cost), (double)cost);
}

