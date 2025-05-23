#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parser.h"

#define BUF_SIZE 1024
#define BASE_CAPACITY 8
#define CAPACITY_MULTIPLIER 2

struct background_jobs {
  pid_t* pids;
  int count;
  int capacity;
};

static void background_jobs_init(struct background_jobs* bg_jobs) {
  bg_jobs->pids = NULL;
  bg_jobs->count = 0;
  bg_jobs->capacity = 0;
}

static void background_jobs_add(struct background_jobs* bg_jobs, pid_t pid) {
  if (bg_jobs->count == bg_jobs->capacity) {
    int new_capacity = bg_jobs->capacity
                           ? bg_jobs->capacity * CAPACITY_MULTIPLIER
                           : BASE_CAPACITY;
    pid_t* new_pids = realloc(bg_jobs->pids, new_capacity * sizeof(pid_t));

    if (!new_pids) {
      perror("realloc");
      exit(EXIT_FAILURE);
    }

    bg_jobs->pids = new_pids;
    bg_jobs->capacity = new_capacity;
  }

  bg_jobs->pids[bg_jobs->count++] = pid;
}

static void background_jobs_remove(struct background_jobs* bg_jobs, int idx) {
  if (idx < 0 || idx >= bg_jobs->count) {
    return;
  }

  for (int i = idx; i < bg_jobs->count - 1; ++i) {
    bg_jobs->pids[i] = bg_jobs->pids[i + 1];
  }

  bg_jobs->count--;
}

static void background_jobs_free(struct background_jobs* bg_jobs) {
  free(bg_jobs->pids);
  bg_jobs->pids = NULL;
  bg_jobs->count = 0;
  bg_jobs->capacity = 0;
}

static void check_background_processes(struct background_jobs* bg_jobs) {
  for (int i = 0; i < bg_jobs->count;) {
    int status;
    pid_t result = waitpid(bg_jobs->pids[i], &status, WNOHANG);
    if (result > 0) {
      background_jobs_remove(bg_jobs, i);
    } else {
      ++i;
    }
  }
}

static const struct expr* find_next_logic(const struct expr* e) {
  const struct expr* first_and = NULL;
  while (e) {
    if (e->type == EXPR_TYPE_OR) {
      return e;
    }
    if (!first_and && e->type == EXPR_TYPE_AND) {
      first_and = e;
    }
    e = e->next;
  }
  return first_and;
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

static void execute_pipeline(const struct expr* start, const struct expr* end,
                             const struct command_line* line, int* exit_code,
                             bool* need_exit, struct background_jobs* bg_jobs) {
  int pipe_fd[2];
  int input_fd = STDIN_FILENO;
  const struct expr* e = start;
  pid_t* pids = NULL;
  int pid_count = 0;
  int pid_capacity = 0;
  int last_status = 0;

  while (e != end) {
    if (e->type == EXPR_TYPE_PIPE) {
      e = e->next;
      continue;
    }

    int use_pipe =
        (e->next != end && e->next && e->next->type == EXPR_TYPE_PIPE);
    if (use_pipe && pipe(pipe_fd) == -1) {
      perror("pipe");
      *exit_code = EXIT_FAILURE;
      free(pids);
      return;
    }

    pid_t pid = fork();
    if (pid == -1) {
      perror("fork");
      *exit_code = EXIT_FAILURE;
      free(pids);
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
      } else if (line->out_type == OUTPUT_TYPE_FILE_NEW ||
                 line->out_type == OUTPUT_TYPE_FILE_APPEND) {
        int flags = (line->out_type == OUTPUT_TYPE_FILE_NEW)
                        ? O_CREAT | O_WRONLY | O_TRUNC
                        : O_CREAT | O_WRONLY | O_APPEND;
        int output_fd = open(line->out_file, flags, 0644);
        if (output_fd == -1) {
          perror("open");
          exit(EXIT_FAILURE);
        }
        dup2(output_fd, STDOUT_FILENO);
        close(output_fd);
      }

      if (use_pipe) {
        close(pipe_fd[0]);
        close(pipe_fd[1]);
      }

      if (strcmp(e->cmd.exe, "exit") == 0) {
        int code = (e->cmd.arg_count > 0) ? atoi(e->cmd.args[0]) : 0;
        *exit_code = code;
        *need_exit = true;
        _exit(code);
      }

      char** args = build_command_args(e);
      execvp(args[0], args);
      perror("execvp");
      free(args);
      exit(EXIT_FAILURE);
    } else {
      if (line->is_background) {
        background_jobs_add(bg_jobs, pid);
      } else {
        if (pid_count == pid_capacity) {
          int new_capacity =
              pid_capacity ? pid_capacity * CAPACITY_MULTIPLIER : BASE_CAPACITY;
          pid_t* new_pids = realloc(pids, new_capacity * sizeof(pid_t));
          if (!new_pids) {
            perror("realloc");
            free(pids);
            exit(EXIT_FAILURE);
          }
          pids = new_pids;
          pid_capacity = new_capacity;
        }
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
  free(pids);

  *exit_code = last_status;
}

static void execute_command_line(const struct command_line* line,
                                 int* exit_code, bool* need_exit,
                                 struct background_jobs* bg_jobs) {
  assert(line != NULL);

  const struct expr* e = line->head;
  int last_status = 0;
  bool local_need_exit = false;

  while (e) {
    const struct expr* logic = find_next_logic(e);
    execute_pipeline(e, logic, line, &last_status, &local_need_exit, bg_jobs);

    if (local_need_exit) {
      *exit_code = last_status;
      *need_exit = true;
      return;
    }

    if (!logic) {
      break;
    }

    if (logic->type == EXPR_TYPE_AND) {
      if (last_status != 0) {
        e = logic->next;
        while (e && e->type != EXPR_TYPE_AND && e->type != EXPR_TYPE_OR) {
          e = e->next;
        }
        continue;
      }
    } else if (logic->type == EXPR_TYPE_OR) {
      if (last_status == 0) {
        e = logic->next;
        while (e && e->type != EXPR_TYPE_AND && e->type != EXPR_TYPE_OR) {
          e = e->next;
        }
        continue;
      }
    }

    e = logic->next;
  }

  *exit_code = last_status;
  *need_exit = local_need_exit;
}

int main(void) {
  char buf[BUF_SIZE];
  int rc;
  struct parser* p = parser_new();
  int exit_code = 0;
  bool need_exit = false;
  struct background_jobs bg_jobs;
  background_jobs_init(&bg_jobs);

  while ((rc = read(STDIN_FILENO, buf, BUF_SIZE)) > 0) {
    parser_feed(p, buf, rc);
    struct command_line* line = NULL;

    while (true) {
      enum parser_error err = parser_pop_next(p, &line);
      if (err == PARSER_ERR_NONE && line == NULL) break;
      if (err != PARSER_ERR_NONE) {
        printf("Error: %d\n", (int)err);
        continue;
      }

      if (line->head && line->head->type == EXPR_TYPE_COMMAND) {
        struct command* first_cmd = &line->head->cmd;
        struct expr* next = line->head->next;

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
        } else if (strcmp(first_cmd->exe, "exit") == 0 &&
                   (!next || next->type != EXPR_TYPE_PIPE)) {
          exit_code = (first_cmd->arg_count > 0) ? atoi(first_cmd->args[0]) : 0;
          command_line_delete(line);
          parser_delete(p);
          check_background_processes(&bg_jobs);
          background_jobs_free(&bg_jobs);
          return exit_code;
        }
      }

      execute_command_line(line, &exit_code, &need_exit, &bg_jobs);
      command_line_delete(line);
      check_background_processes(&bg_jobs);
      if (need_exit) {
        background_jobs_free(&bg_jobs);
        parser_delete(p);
        return exit_code;
      }
    }
  }

  background_jobs_free(&bg_jobs);
  parser_delete(p);
  return exit_code;
}
