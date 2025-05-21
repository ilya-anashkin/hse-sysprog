#include "parser.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>

#define BUF_SIZE 1024
#define MAX_BG_PROCESSES 128

struct background_jobs {
	pid_t pids[MAX_BG_PROCESSES];
	int count;
};

// static void debug_command_line(const struct command_line* line)
// {
// 	assert(line != NULL);
// 	printf("================================\n");
// 	printf("Command line:\n");
// 	printf("Is background: %d\n", (int)line->is_background);
// 	printf("Output: ");
// 	if (line->out_type == OUTPUT_TYPE_STDOUT) {
// 		printf("stdout\n");
// 	} else if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
// 		printf("new file - \"%s\"\n", line->out_file);
// 	} else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
// 		printf("append file - \"%s\"\n", line->out_file);
// 	} else {
// 		assert(false);
// 	}
// 	printf("Expressions:\n");
// 	const struct expr* e = line->head;
// 	while (e != NULL) {
// 		if (e->type == EXPR_TYPE_COMMAND) {
// 			printf("\tCommand: %s", e->cmd.exe);
// 			for (uint32_t i = 0; i < e->cmd.arg_count; ++i)
// 				printf(" %s", e->cmd.args[i]);
// 			printf("\n");
// 		} else if (e->type == EXPR_TYPE_PIPE) {
// 			printf("\tPIPE\n");
// 		} else if (e->type == EXPR_TYPE_AND) {
// 			printf("\tAND\n");
// 		} else if (e->type == EXPR_TYPE_OR) {
// 			printf("\tOR\n");
// 		} else {
// 			assert(false);
// 		}
// 		e = e->next;
// 	}
// }

static void check_background_processes(struct background_jobs* bg_jobs) {
	for (int i = 0; i < bg_jobs->count; i++) {
		if (bg_jobs->pids[i] > 0) {
			int status;
			pid_t result = waitpid(bg_jobs->pids[i], &status, WNOHANG);
			if (result > 0) {
				bg_jobs->pids[i] = 0;
			}
		}
	}
}

static char** build_command_args(const struct expr* e) {
	char** args = malloc((e->cmd.arg_count + 2) * sizeof(char*));
	if (!args) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}

	args[0] = e->cmd.exe;
	for (uint32_t i = 0; i < e->cmd.arg_count; ++i) {
		args[i + 1] = e->cmd.args[i];
	}
	args[e->cmd.arg_count + 1] = NULL;

	return args;
}

static void execute_command_line(const struct command_line* line, int* exit_code, bool* need_exit, struct background_jobs* bg_jobs) {
	assert(line != NULL);

	// debug_command_line(line);

	int pipe_fd[2];
	int input_fd = STDIN_FILENO;
	const struct expr* e = line->head;
	pid_t pids[BUF_SIZE];
	int pid_count = 0;
	int last_status = 0;

	while (e != NULL) {
		if (e->type == EXPR_TYPE_PIPE) {
			e = e->next;
			continue;
		}

		int use_pipe = (e->next != NULL);
		if (use_pipe && pipe(pipe_fd) == -1) {
			perror("pipe");
			*exit_code = EXIT_FAILURE;
			return;
		}

		pid_t pid = fork();
		if (pid == -1) {
			perror("fork");
			*exit_code = EXIT_FAILURE;
			return;
		}

		if (pid == 0) {
			if (input_fd != STDIN_FILENO) {
				dup2(input_fd, STDIN_FILENO);
				close(input_fd);
			}

			if (use_pipe) {
				dup2(pipe_fd[1], STDOUT_FILENO);
				close(pipe_fd[1]);
			} else if (line->out_type == OUTPUT_TYPE_FILE_NEW || line->out_type == OUTPUT_TYPE_FILE_APPEND) {
				int flags = (line->out_type == OUTPUT_TYPE_FILE_NEW) ? O_CREAT | O_WRONLY | O_TRUNC : O_CREAT | O_WRONLY | O_APPEND;
				int output_fd = open(line->out_file, flags, 0644);
				if (output_fd == -1) {
					perror("open");
					*exit_code = EXIT_FAILURE;
					return;
				}
				dup2(output_fd, STDOUT_FILENO);
				close(output_fd);
			}

			close(pipe_fd[0]);
			close(pipe_fd[1]);

			if (strcmp(e->cmd.exe, "exit") == 0) {
				int code = (e->cmd.arg_count > 0) ? atoi(e->cmd.args[0]) : 0;
				*exit_code = code;
				*need_exit = true;
				return;
			}

			char** args = build_command_args(e);
			execvp(args[0], args);
			perror("execvp");
			free(args);
			*exit_code = EXIT_FAILURE;
			return;
		} else {
			if (line->is_background) {
				if (bg_jobs->count < MAX_BG_PROCESSES) {
					bg_jobs->pids[bg_jobs->count++] = pid;
				} else {
					perror("Too many background processes");
				}
			} else {
				pids[pid_count++] = pid;
			}

			if (use_pipe) {
				close(pipe_fd[1]);
			}
			if (input_fd != STDIN_FILENO) {
				close(input_fd);
			}
			input_fd = use_pipe ? pipe_fd[0] : STDIN_FILENO;
		}

		e = e->next;
	}

	int status;
	for (int i = 0; i < pid_count; i++) {
		waitpid(pids[i], &status, 0);
		if (WIFEXITED(status)) {
			last_status = WEXITSTATUS(status);
		}
	}

	*exit_code = last_status;
}

int main(void) {
	char buf[BUF_SIZE];
	int rc;
	struct parser* p = parser_new();
	int exit_code = 0;
	bool need_exit = false;
	struct background_jobs bg_jobs = { .count = 0 };

	while ((rc = read(STDIN_FILENO, buf, BUF_SIZE)) > 0) {
		parser_feed(p, buf, rc);
		struct command_line* line = NULL;

		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				printf("Error: %d\n", (int)err);
				continue;
			}

			if (line->head && line->head->type == EXPR_TYPE_COMMAND) {
				struct command* first_cmd = &line->head->cmd;

				if (strcmp(first_cmd->exe, "cd") == 0) {
					if (first_cmd->arg_count > 0) {
						if (chdir(first_cmd->args[0]) != 0) {
							perror("chdir failed");
						}
					} else {
						fprintf(stderr, "cd: missing argument\n");
					}
					command_line_delete(line);
					continue;
				} else if (strcmp(first_cmd->exe, "exit") == 0 && line->head->next == NULL) {
					exit_code = (first_cmd->arg_count > 0) ? atoi(first_cmd->args[0]) : 0;
					command_line_delete(line);
					parser_delete(p);
					check_background_processes(&bg_jobs);
					return exit_code;
				}
			}

			execute_command_line(line, &exit_code, &need_exit, &bg_jobs);
			command_line_delete(line);
			check_background_processes(&bg_jobs);
			if (need_exit) {
				parser_delete(p);
				return exit_code;
			}
		}
	}

	parser_delete(p);
	return exit_code;
}
