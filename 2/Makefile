GCC_FLAGS = -Wextra -Werror -Wall -Wno-gnu-folding-constant

all:
# use a.out for testing checker
	gcc $(GCC_FLAGS) solution.c parser.c -o a.out

hh:
# use a.out for testing checker
	gcc $(GCC_FLAGS) solution.c parser.c ../utils/heap_help/heap_help.c -ldl -rdynamic -o a.out

# For automatic testing systems to be able to just build whatever was submitted
# by a student.
test_glob:
	gcc $(GCC_FLAGS) *.c -o mybash
