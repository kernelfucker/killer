# killer
dd-like utility

# compile
clang killer.c -o killer

# usage
$ ./killer status=verbose status=progress if=/dev/zero of=image.img bs=1M cn=512

$ ./killer oflag=sync workers=1 if=/dev/zero of=image.img bs=1M cn=512

# options
base options:

`  if=file               input file`

  `of=file               output file`
  
  `bs=bytes              block size`
  
  `cn=x                  copy only x input blocks`
  
  `sp=x                  skip x input blocks`
  
  `sk=x                  skip x output blocks`

conv options:

  `conv=notrunc          do not truncate output`
  
  `conv=sync             buffer with zeros on erros`
  
  `conv=noerror          continue after errors, like sp`
  
  `conv=swap             swap byte order`
  
  `conv=pattern          fill with pattern`
  
  `conv=verify           verify writes`

oflags options:

  `oflag=sync            sync writes`
  
  `oflag=atomic          atomic replacement like order`

innovative options:

  `workers=x             number of paralel workers, 1-20`
  
  `errors=x              max allowed last errors, default is 10`
  
  `pattern=hex           fill patern well hex, default is 0x00000000`

status options:

  `status=verbose        verbose option`
  
  `status=progress       show progress`

# note
**default block size 4mb**

*have fun :)*

# example
![image](https://github.com/user-attachments/assets/d5f880e6-2ff2-4c59-86ca-50c16a36ae26)
