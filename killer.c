/* See LICENSE file for license details */
/* killer - dd-like utility */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#define dblksize (4 * 1024 * 1024)
#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))
#define maxerrs 10
#define maxwers 20

typedef struct{
	char *inf;		// infile
	char *outf;		// outfile
	size_t bs;		// block size
	size_t cn;		// count
	size_t sp;		// skip
	size_t sk;		// seek
	int notrunc;		// conv=notrunc
	int sync;		// conv=sync
	int noerror;		// conv=noerror
	int swap;		// conv=swap
	int patternf;		// conv=pattern
	int sparse;		// conv=sparse
	int verify;		// conv=verify
	int atomic;		// oflag=atomic
	int fsync;		// oflag=sync
	int workers;		// workers=x
	int errors;		// errors=x
	int verbose;		// status=verbose
	int progress;		// status=progress
	uint32_t pattern;	// pattern=0x00000000
} Opts;

volatile size_t sparsebs = 0;
volatile size_t totalbs = 0;
volatile size_t totalers = 0;
volatile sig_atomic_t interpted = 0;
volatile sig_atomic_t statsrs = 0;

time_t st_time;

volatile uint64_t df_blockcn = 0;

pthread_mutex_t statmt = PTHREAD_MUTEX_INITIALIZER;

void handle(int s){
	if(statsrs)interpted = 1;
	else statsrs = 1;
}

void d(const char *m){
	perror(m);
	exit(EXIT_FAILURE);
}

void usage(){
	fprintf(stderr, "killer - dd-like utility\n");
	fprintf(stderr, "usage: killer [options]..\n");
	fprintf(stderr, "base options:\n");
	fprintf(stderr, "  if=file		input file\n");
	fprintf(stderr, "  of=file		output file\n");
	fprintf(stderr, "  bs=bytes		block size\n");
	fprintf(stderr, "  cn=x			copy only x input blocks\n");
	fprintf(stderr, "  sp=x			skip x input blocks\n");
	fprintf(stderr, "  sk=x			skip x output blocks\n");
	fprintf(stderr, "conv options:\n");
	fprintf(stderr, "  conv=notrunc		do not truncate output\n");
	fprintf(stderr, "  conv=sync		buffer with zeros on erros\n");
	fprintf(stderr, "  conv=noerror		continue after errors, like sp\n");
	fprintf(stderr, "  conv=swap		swap byte order\n");
	fprintf(stderr, "  conv=pattern		fill with pattern\n");
	fprintf(stderr, "  conv=verify		verify writes\n");
	fprintf(stderr, "oflags options:\n");
	fprintf(stderr, "  oflag=sync		sync writes\n");
	fprintf(stderr, "  oflag=atomic		atomic replacement like order\n");
	fprintf(stderr, "innovative options:\n");
	fprintf(stderr, "  workers=x		number of paralel workers, 1-20\n");
	fprintf(stderr, "  errors=x		max allowed last errors, default is 10\n");
	fprintf(stderr, "  pattern=hex		fill patern well hex, default is 0x00000000\n");
	fprintf(stderr, "status options:\n");
	fprintf(stderr, "  status=verbose	verbose option\n");
	fprintf(stderr, "  status=progress	show progress\n");
	fprintf(stderr, "help:\n");
	fprintf(stderr, "  --help		display this\n");
	exit(EXIT_FAILURE);
}

void pargs(int argc, char **argv, Opts *opts){
	memset(opts, 0, sizeof(Opts));
	opts->bs = dblksize;
	opts->errors = maxerrs;
	opts->workers = 1;
	for(int i = 1; i < argc; i++){
		char *a = argv[i];
		if(strncmp(a, "if=", 3) == 0){
			opts->inf = (strcmp(a + 3, "-") == 0) ? NULL : a + 3;
		} else if(strncmp(a, "of=", 3) == 0){
			opts->outf = (strcmp(a + 3, "-") == 0) ? NULL : a + 3;
		} else if(strncmp(a, "bs=", 3) == 0){
			char *end;
			opts->bs = strtol(a + 3, &end, 10);
			if(*end == 'M'){
				opts->bs *= 1024 * 1024;
			} else if(*end == 'K'){
				opts->bs *= 1024;
			} else if(*end != '\0'){
				fprintf(stderr, "invalid block size format: %s\n", a);
				exit(EXIT_FAILURE);
			}

		} else if(strncmp(a, "cn=", 3) == 0){
			opts->cn = atol(a + 3);
		} else if(strncmp(a, "sp=", 5) == 0){
			opts->sp = atol(a + 5);
		} else if(strncmp(a, "sk=", 5) == 0){
			opts->sk = atol(a + 5);
		} else if(strncmp(a, "workers=", 8) == 0){
			opts->workers = atoi(a + 8);
			if(opts->workers < 1) opts->workers = 1;
			if(opts->workers > maxwers) opts->workers = maxwers;
		} else if(strncmp(a, "errors=", 11) == 0){
			opts->errors = atoi(a + 11);
		} else if(strncmp(a, "pattern=", 8) == 0){
			opts->patternf = 1;
			opts->pattern = strtoul(a + 8, NULL, 16);
		} else if(strcmp(a, "conv=notrunc") == 0){
			opts->notrunc = 1;
		} else if(strcmp(a, "conv=sync") == 0){
			opts->sync = 1;
		} else if(strcmp(a, "conv=noerror") == 0){
			opts->noerror = 1;
		} else if(strcmp(a, "conv=sparse") == 0){
			opts->sparse = 1;
		} else if(strcmp(a, "conv=verify") == 0){
			opts->verify = 1;
		} else if(strcmp(a, "conv=swap") == 0){
			opts->swap = 1;
		} else if(strcmp(a, "conv=pattern") == 0){
			opts->patternf = 1;
		} else if(strcmp(a, "oflag=sync") == 0){
			opts->fsync = 1;
		} else if(strcmp(a, "oflag=atomic") == 0){
			opts->atomic = 1;
		} else if(strcmp(a, "status=progress") == 0){
			opts->progress = 1;
		} else if(strcmp(a, "status=verbose") == 0){
			opts->verbose = 1;
			opts->progress = 1;
		} else if(strcmp(a, "--help") == 0){
			usage();
		}
	}

	if(opts->verbose){
		fprintf(stderr, "input: %s\n", opts->inf ? opts->inf : "stdin");
		fprintf(stderr, "output: %s\n", opts-> outf ? opts->outf : "stdout");
		fprintf(stderr, "block size: %zu bytes\n", opts->bs);
		if(opts->cn) fprintf(stderr, "count: %zu blocks\n", opts->cn);
		if(opts->sp) fprintf(stderr, "skip: %zu input blocks\n", opts->sp);
		if(opts->sk) fprintf(stderr, "seek: %zu output blocks\n", opts->sk);
		if(opts->workers > 1) fprintf(stderr, "workers: %d\n", opts->workers);
	}
}

void prstats(){
	double e = difftime(time(NULL), st_time);
	double d = (totalbs / (1024.0 * 1024.0)) / max(e, 0.001);
	fprintf(stderr, "\r%zu bytes / (%.2f mb) copied, %.2f s, %.2f mb/s", totalbs, totalbs / (1024.0 * 1024.0), e, d);
	if(sparsebs > 0){
		fprintf(stderr, ", sparse: %zu bytes", sparsebs);
	}

	fflush(stderr);
}

void *abfr(size_t s){
	void *p = malloc(s);
	if(!p) d("malloc failed");
	return p;
}

int is_zbfr(const char *buf, size_t s){
	for(size_t i = 0; i < s; i++){
		if(buf[i] != 0) return 0;
	}

	return 1;
}

void swapbs(char *buf, size_t s){
	for(size_t i = 0; i < s / 2; i++){
		char t = buf[i];
		buf[i] = buf[s - 1 - i];
		buf[s - 1 - i] = t;
	}
}

void pf(char *buf, size_t s, uint32_t pattern){
	for(size_t i = 0; i < s; i++){
		buf[i] = ((char *)&pattern)[i % sizeof(uint32_t)];
	}
}

typedef struct{
	int fdin;
	int fdout;
	uint64_t blockcn;
	Opts *opts;
} wersargs;

void *workerth(void *a){
	wersargs *args = (wersargs *)a;
	Opts *opts = args->opts;
	char *buf = abfr(opts->bs);
	while(!interpted && (opts->cn == 0 || args->blockcn < opts->cn)){
		uint64_t currblock = __sync_fetch_and_add(&df_blockcn, 1);		
		off_t offset = (opts->sp + currblock) * opts->bs;
		if(lseek(args->fdin, offset, SEEK_SET) == -1){
			perror("lsk(lseek) input failed");
			__sync_fetch_and_add(&totalers, 1);
			if(totalers >= opts->errors) break;
			continue;
		}

		ssize_t rdbytes = read(args->fdin, buf, opts->bs);
		if(rdbytes == -1){
			perror("read failed");
			__sync_fetch_and_add(&totalers, 1);
			if(totalers >= opts->errors || !opts->noerror) break;
			continue;
		}

		if(rdbytes == 0) break;
		if(opts->sync && rdbytes < opts->bs){
			memset(buf + rdbytes, 0, opts->bs - rdbytes);
			rdbytes = opts->bs;
		}

		if(opts->swap) swapbs(buf, rdbytes);
		if(opts->patternf) pf(buf, rdbytes, opts->pattern);

		int is_zero = opts->sparse && is_zbfr(buf, rdbytes);
		if(is_zero){
			if(lseek(args->fdout, rdbytes, SEEK_CUR) == -1){
				perror("lsk(lseek) output failed");
				__sync_fetch_and_add(&totalers, 1);
				if(totalers >= opts->errors) break;
				continue;
			}

			pthread_mutex_lock(&statmt);
			totalbs += rdbytes;
			sparsebs += rdbytes;
			pthread_mutex_unlock(&statmt);
			continue;
		}

		off_t out_offset = (opts->sk * opts->bs) + (currblock * opts->bs);
		if(lseek(args->fdout, out_offset, SEEK_SET) == -1){
			perror("lsk(lseek) output failed");
			__sync_fetch_and_add(&totalers, 1);
			if(totalers >= opts->errors) break;
			continue;
		}

		ssize_t wrbytes = write(args->fdout, buf, rdbytes);
		if(wrbytes == -1){
			perror("write failed");
			__sync_fetch_and_add(&totalers, 1);
			if(totalers >= opts->errors) break;
			continue;
		}

		if(opts->verify){
			char *verifybuf = abfr(wrbytes);
			if(lseek(args->fdout, out_offset, SEEK_SET) == -1 ||
				read(args->fdout, verifybuf, wrbytes) != wrbytes ||
				memcmp(buf, verifybuf, wrbytes) != 0){
					fprintf(stderr, "verify failed at offset %lld\n", (long long)out_offset);
					__sync_fetch_and_add(&totalers, 1);
					free(verifybuf);
					if(totalers >= opts->errors) break;
					continue;
				}

				free(verifybuf);

				}

			pthread_mutex_lock(&statmt);
			totalbs += wrbytes;
			pthread_mutex_unlock(&statmt);
	}

	free(buf);
	return NULL;
}

void copy(Opts *opts){
	int flin = O_RDONLY;
	int flout = O_WRONLY|O_CREAT;
	if(!opts->notrunc) flout |= O_TRUNC;
	if(opts->fsync) flout |= O_SYNC;
	if(opts->atomic) flout |= O_EXCL;
	int fdin = opts->inf ? open(opts->inf, flin) : STDIN_FILENO;
	if(fdin == -1) d("failed to open input file");
	int fdout = opts->outf ? open(opts->outf, flout, 0644) : STDOUT_FILENO;
	if(fdout == -1) d("failed to open output file");
	if(opts->sp && lseek(fdin, opts->sp * opts->bs, SEEK_SET) == -1)
		d("failed to skip input");
	if(opts->sk && lseek(fdout, opts->sk * opts->bs, SEEK_SET) == -1)
		d("failed to seek output");

	st_time = time(NULL);
	signal(SIGINT, handle);
	if(opts->workers > 1){
		pthread_t threads[maxerrs];
		wersargs args = {fdin, fdout, 0, opts};
		for(int i = 0; i < opts->workers; i++){
			if(pthread_create(&threads[i], NULL, workerth, &args) != 0){
				d("failed to create worker thread");
			}
		}

		while(!interpted){
			if(opts->progress) prstats();
			int a = 0;
			for(int i = 0; i < opts->workers; i++){
				if(pthread_kill(threads[i], 0) == 0){
					a = 1;
					break;
				}
			}

			if(!a) break;
		}

		for(int i = 0; i < opts->workers; i++){
			pthread_join(threads[i], NULL);
		}
	} else {
		char *buf = abfr(opts->bs);
		size_t cpblocks = 0;
		size_t lastupdt = st_time;
		while(!interpted && (opts->cn == 0 || cpblocks < opts->cn)){
			ssize_t rdbytes = read(fdin, buf, opts->bs);
			if(rdbytes == -1){
				if(opts->noerror){
					fprintf(stderr, "read error, continuing: %s\n", strerror(errno));
					totalers++;
					if(totalers >= opts->errors) break;
					continue;
				} else d("read failed");
			}

			if(rdbytes == 0) break;
			if(opts->sync && rdbytes < opts->bs){
				memset(buf + rdbytes, 0, opts->bs - rdbytes);
				rdbytes = opts->bs;
			}

			if(opts->swap) swapbs(buf, rdbytes);
			if(opts->patternf) pf(buf, rdbytes, opts->pattern);
			int is_zero = opts->sparse && is_zbfr(buf, rdbytes);
			if(is_zero){
				if(lseek(fdout, rdbytes, SEEK_CUR) == -1){
					d("lsk(lseek) failed for sparse block");
				}

				totalbs += rdbytes;
				sparsebs += rdbytes;
				cpblocks++;
				continue;
			}

			ssize_t wrbytes = write(fdout, buf, rdbytes);
			if(wrbytes == -1)
				d("write failed");
			if(opts->verify){
				off_t offset = lseek(fdout, -wrbytes, SEEK_CUR);
				char *verifybuf = abfr(wrbytes);
				if(read(fdout, verifybuf, wrbytes) != wrbytes ||
					memcmp(buf, verifybuf, wrbytes) != 0){
						d("verify failed");
					}

				free(verifybuf);
				lseek(fdout, offset, SEEK_SET);
			}

			totalbs += wrbytes;
			cpblocks++;
			time_t t = time(NULL);
			if(opts->progress && difftime(t, lastupdt) >= 1.0){
				prstats();
				lastupdt = t;
			}
		}

		free(buf);
	}

	if(opts->fsync && fsync(fdout) == -1){
		perror("fsync failed");
	}

	double e = difftime(time(NULL), st_time);
	if(opts->progress || opts->verbose){
		fprintf(stderr, "\n");
		prstats();
		fprintf(stderr, "\ntotal time: %.2f seconds\n", e);
	} else {
		fprintf(stderr, "operation completed in %.2f seconds\n", e);
	}

	close(fdin);
	close(fdout);
}

int main(int argc, char **argv){
	Opts opts;
	pargs(argc, argv, &opts);
	copy(&opts);
	return interpted ? EXIT_FAILURE : EXIT_SUCCESS;
}
