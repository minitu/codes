/*
 *
 * NODES - NODe-aware Exascale Simulation
 * --------------------------------------
 * Adapted from MPI-replay in CODES
 * Developed by Jaemin Choi <jaemin@acm.org>
 *
 */
#include <ross.h>

#include <inttypes.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <time.h>

#include "codes/codes-workload.h"
#include "codes/codes.h"
#include "codes/configuration.h"
#include "codes/codes_mapping.h"
#include "codes/model-net.h"
#include "codes/rc-stack.h"
#include "codes/quicklist.h"
#include "codes/quickhash.h"
#include "codes/codes-jobmap.h"

#define CONTROL_MSG_SZ 64 // Size of control messages used in rendezvous protocol
#define MAX_WAIT_REQS 1024
#define RANK_HASH_TABLE_SZ 2000
#define MAX_STATS 65536
#define COL_TAG 1235 // Tag for collectives
#define BAR_TAG 1234 // Tag for barriers

// NOTE: Message tracking currently only works in sequential mode.
// Reverse computation has not been implemented.
static int enable_msg_tracking = 0;
static int msg_size_hash_compare(void* key, struct qhash_head* link);

/* NOTE: Message tracking works in sequential mode only! */
static int debug_cols = 0;
/* Turning on this option slows down optimistic mode substantially. Only turn
 * on if you get issues with wait-all completion with traces. */
static double compute_time_speedup = 1;
int nprocs = 0;
static int unmatched = 0;
char workload_type[128];
char workload_file[8192];
static int wrkld_id;
static int num_net_traces = 0;
static int priority_type = 0;
static int num_dumpi_traces = 0;
static int64_t eager_limit = 8388608;

static long num_ops = 0;
static int upper_threshold = 1048576;
static int alloc_spec = 0; // Allocation file specified?
static tw_stime self_overhead = 0.0;

/* Doing LP IO*/
static char* params = NULL;
static char lp_io_dir[256] = {'\0'};
static lp_io_handle io_handle;
static unsigned int lp_io_use_suffix = 0;
static int do_lp_io = 0;

// Variables for loading multiple applications
char alloc_file[8192];
int num_traces_of_job[5];
char file_name_of_job[5][8192];

tw_stime soft_delay_mpi = 0;
tw_stime nic_delay = 0;
tw_stime copy_per_byte_eager = 0.0;

struct codes_jobmap_ctx *jobmap_ctx = NULL;
struct codes_jobmap_params_list jobmap_p;

// Variables for Cortex Support
#ifdef ENABLE_CORTEX_PYTHON
static char cortex_file[512] = "\0";
static char cortex_class[512] = "\0";
static char cortex_gen[512] = "\0";
#endif

typedef struct nw_state nw_state;
typedef struct nw_message nw_message;
typedef unsigned int dumpi_req_id;

static int net_id = 0;
static float noise = 1.0; // Used in creating random time values
static int total_ranks = 0, ranks_per_node = 0;

// Output log files
FILE* debug_log_file = NULL;
FILE* msg_log_file = NULL;
FILE* agg_log_file = NULL;
FILE* meta_log_file = NULL;

static uint64_t sample_bytes_written = 0;

unsigned long long num_bytes_sent = 0;
unsigned long long num_bytes_recvd = 0;

double max_time = 0, max_comm_time = 0, max_wait_time = 0, max_send_time = 0, max_recv_time = 0;
double avg_time = 0, avg_comm_time = 0, avg_wait_time = 0, avg_send_time = 0, avg_recv_time = 0;

/* runtime option for disabling computation time simulation */
static int disable_compute = 0;
static int enable_sampling = 0;
static double sampling_interval = 5000000;
static double sampling_end_time = 3000000000;
static int enable_debug = 0;

/* set group context */
struct codes_mctx mapping_context;
enum MAPPING_CONTEXTS
{
    GROUP_RATIO=1,
    GROUP_RATIO_REVERSE,
    GROUP_DIRECT,
    GROUP_MODULO,
    GROUP_MODULO_REVERSE,
    UNKNOWN
};
static int mctx_type = GROUP_MODULO;

enum MPI_NW_EVENTS
{
  MPI_OP_GET_NEXT=1,
  MPI_SEND_ARRIVED,
  MPI_SEND_ARRIVED_CB, // For tracking message times on sender
  MPI_SEND_POSTED,
  MPI_REND_ARRIVED,
  MPI_REND_ACK_ARRIVED,
};

struct mpi_workload_sample
{
    /* Sampling data */
    int nw_id;
    int app_id;
    unsigned long num_sends_sample;
    unsigned long num_bytes_sample;
    unsigned long num_waits_sample;
    double sample_end_time;
};

// Data structure for matching MPI operations
struct mpi_msg_queue
{
  int op_type;
  int tag;
  int source_rank;
  int dest_rank;
  int64_t num_bytes;
  int64_t seq_id;
  tw_stime req_init_time;
  dumpi_req_id req_id;
  struct qlist_head ql;
};

/* stores request IDs of completed MPI operations (Isends or Irecvs) */
struct completed_requests
{
	unsigned int req_id;
    struct qlist_head ql;
    int index;
};

/* for wait operations, store the pending operation and number of completed waits so far. */
struct pending_waits
{
    int op_type;
    unsigned int req_ids[MAX_WAIT_REQS];
	int num_completed;
	int count;
    tw_stime start_time;
    struct qlist_head ql;
};

// Information per tracked MPI message
struct msg_size_info
{
  int64_t msg_size;
  int num_msgs;
  tw_stime agg_latency;
  tw_stime avg_latency;
  struct qhash_head hash_link;
  struct qlist_head ql;
};

struct ross_model_sample
{
    tw_lpid nw_id;
    int app_id;
    int local_rank;
    unsigned long num_sends;
    unsigned long num_recvs;
    unsigned long long num_bytes_sent;
    unsigned long long num_bytes_recvd;
    double send_time;
    double recv_time;
    double wait_time;
    double compute_time;
    double comm_time;
    double max_time;
};

typedef struct mpi_msg_queue mpi_msg_queue;
typedef struct completed_requests completed_requests;
typedef struct pending_waits pending_waits;

// State of the network LP.
// It contains the pointers to send/receive lists.
struct nw_state
{
  long num_events_per_lp;
  tw_lpid nw_id;
  short wrkld_end;
  int app_id;
  int local_rank;

  int is_finished;

  // Reverse computation stacks
  struct rc_stack* rc_processed_ops;
  struct rc_stack* rc_processed_wait_op;
  struct rc_stack* rc_matched_reqs;

  // Count of sends, receives, collectives and delays
  unsigned long num_sends;
  unsigned long num_recvs;
  unsigned long num_cols;
  unsigned long num_delays;
  unsigned long num_wait;
  unsigned long num_waitall;
  unsigned long num_waitsome;

  // Timing values
  double start_time;
  double col_time;
  double reduce_time;
  int num_reduce;
  double all_reduce_time;
  int num_all_reduce;
  double elapsed_time;
  double compute_time;
  double send_time;
  double max_time;
  double recv_time;
  double wait_time;

  // For handling message arrivals & receives (from workload/trace)
  struct qlist_head arrival_queue; // Messages arrived due to MPI_Send from source
  struct qlist_head pending_recvs_queue; // Unmatched receives (read from workload)

  // For handling waits
  struct qlist_head completed_reqs; // List of completed send/recv requests
  struct pending_waits* wait_op; // Pending wait operations

  // Message latency information per size
  struct qhash_table* msg_sz_table;
  struct qlist_head msg_sz_list;

  unsigned long long num_bytes_sent;
  unsigned long long num_bytes_recvd;

  // For sampling data
  tw_stime cur_interval_end;
  int sampling_indx;
  int max_arr_size;
  struct mpi_workload_sample* mpi_wkld_samples;
  char output_buf[512];
  char col_stats[64];
  struct ross_model_sample ross_sample;
};

// Data structure for handling reverse computation (RC).
// TODO: Fill this data structure only when the simulation runs in optimistic mode
struct nw_message
{
  int msg_type;
  int op_type; // Workload type (CODES_WK_*)
  model_net_event_return event_rc;
  struct codes_workload_op* mpi_op;

  struct
  {
    tw_lpid src_rank;
    int dest_rank;
    int64_t num_bytes;
    int num_matched;
    int data_type;
    double sim_start_time;
    // for callbacks - time message was received
    double msg_send_time;
    unsigned int req_id;
    int matched_req;
    int tag;
    int app_id;
    int found_match;
    short wait_completed;
    short rend_send;
  } fwd;

  struct
  {
    double saved_send_time;
    double saved_send_time_sample;
    double saved_recv_time;
    double saved_recv_time_sample;
    double saved_wait_time;
    double saved_wait_time_sample;
    double saved_delay;
    double saved_delay_sample;
    int64_t saved_num_bytes;
    double saved_prev_max_time;
  } rc;
};

static void send_ack_back(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp, mpi_msg_queue* mpi_op, int matched_req);
static void send_ack_back_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void codes_exec_mpi_send(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp, struct codes_workload_op* mpi_op, int is_rend);
static void codes_exec_mpi_send_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void codes_exec_mpi_recv(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp, struct codes_workload_op* mpi_op);
static void codes_exec_mpi_recv_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void codes_exec_comp_delay(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp, struct codes_workload_op* mpi_op);
static void codes_exec_comp_delay_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void get_next_mpi_operation(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void get_next_mpi_operation_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void issue_next_event(tw_lp* lp);
static void issue_next_event_rc(tw_lp* lp);

// Helper functions for handling MPI message queues
// Upon arrival of local completion message, inserts operation in completed send queue
// Upon arrival of an isend operation, updates the arrival queue of the network
static void update_completed_queue(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp, dumpi_req_id req_id);
static void update_completed_queue_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void update_arrival_queue(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void update_arrival_queue_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
// Callback to message sender for computing message time
static void update_message_time(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);
static void update_message_time_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp);

// Time conversion functions
static tw_stime s_to_ns(tw_stime s)
{
    return (s * (1000.0 * 1000.0 * 1000.0));
}

static tw_stime ns_to_s(tw_stime ns)
{
    return (ns / (1000.0 * 1000.0 * 1000.0));
}

// Update information on this message size.
// Only called when message tracking is enabled.
static void update_message_size(struct nw_state* s, tw_lp* lp, tw_bf* bf,
    struct nw_message* m, mpi_msg_queue* qitem, int is_eager, int is_send)
{
  (void)bf;
  (void)is_eager;

  assert(enable_msg_tracking > 0); // Check if msg tracking is enabled

  tw_stime msg_init_time = qitem->req_init_time;
  if (is_send)
    msg_init_time = m->fwd.sim_start_time;

  // Update hash table
  struct qhash_head* hash_link = qhash_search(s->msg_sz_table, &(qitem->num_bytes));
  if (!hash_link) { // Item not found, create entry for this message size
    struct msg_size_info* msg_info = (struct msg_size_info*)malloc(sizeof(struct msg_size_info));
    msg_info->msg_size = qitem->num_bytes;
    msg_info->num_msgs = 1;
    msg_info->agg_latency = tw_now(lp) - msg_init_time;
    msg_info->avg_latency = msg_info->agg_latency;
    assert(s->msg_sz_table);
    qhash_add(s->msg_sz_table, &(msg_info->msg_size), &(msg_info->hash_link));
    qlist_add(&msg_info->ql, &s->msg_sz_list);
    //printf("\nMsg size %d aggregate latency %f num messages %d", m->fwd.num_bytes, msg_info->agg_latency, msg_info->num_msgs);
  }
  else { // Item found, add current message information to entry
    struct msg_size_info* tmp = qhash_entry(hash_link, struct msg_size_info, hash_link);
    tmp->num_msgs++;
    tmp->agg_latency += tw_now(lp) - msg_init_time;
    tmp->avg_latency = (tmp->agg_latency / tmp->num_msgs);
    //printf("\nMsg size %lld aggregate latency %f num messages %d", qitem->num_bytes, tmp->agg_latency, tmp->num_msgs);
  }
}

// TODO: Reverse computation needed to enable message tracking for parallel simulation
/*
static void update_message_size_rc(struct nw_state* s, tw_lp* lp, tw_bf* bf,
    struct nw_message* m)
{
  (void)s;
  (void)lp;
  (void)bf;
  (void)m;
}
*/

// FIXME: Printing functions may not work properly in parallel mode
static void print_msgs_queue(struct qlist_head* head, int is_send)
{
  if (is_send)
    printf("\nSend msgs queue:\n");
  else
    printf("\nRecv msgs queue:\n");

  struct qlist_head* ent = NULL;
  mpi_msg_queue* current = NULL;
  qlist_for_each(ent, head) {
    current = qlist_entry(ent, mpi_msg_queue, ql);
    printf("source %d dest %d bytes %"PRId64" tag %d\n", current->source_rank,
        current->dest_rank, current->num_bytes, current->tag);
  }
}

static void print_waiting_reqs(struct pending_waits* wait_op)
{
  uint32_t* reqs = wait_op->req_ids;
  int count = wait_op->count;

  printf("\nWaiting reqs: %d count ", count);
  int i;
  for (i = 0; i < count; i++)
    printf("%d ", reqs[i]);
  printf("\n");
}

static void print_completed_queue(tw_lp* lp, struct qlist_head* head)
{
  printf("\nCompleted queue:\n");

  struct qlist_head * ent = NULL;
  struct completed_requests* current = NULL;
  tw_output(lp, "\n");
  qlist_for_each(ent, head) {
    current = qlist_entry(ent, completed_requests, ql);
    tw_output(lp, " %llu ", current->req_id);
  }
}

static int clear_completed_reqs(nw_state* s, tw_lp* lp, unsigned int* reqs, int count)
{
  int i, matched = 0;

  for (i = 0; i < count; i++) {
    struct qlist_head* ent = NULL;
    struct completed_requests* current = NULL;
    struct completed_requests* prev = NULL; // Used to store deleted request for RC

    int index = 0;
    qlist_for_each(ent, &s->completed_reqs) {
      if (prev) {
        rc_stack_push(lp, prev, free, s->rc_matched_reqs);
        prev = NULL;
      }

      current = qlist_entry(ent, completed_requests, ql);
      current->index = index;
      if (current->req_id == reqs[i]) {
        ++matched;
        qlist_del(&current->ql);
        prev = current;
      }
      ++index;
    }

    if (prev) {
      rc_stack_push(lp, prev, free, s->rc_matched_reqs);
      prev = NULL;
    }
  }

  return matched;
}

// Used for RC
static void add_completed_reqs(nw_state* s, tw_lp* lp, int count)
{
  (void)lp;

  for (int i = 0; i < count; i++) {
    struct completed_requests * req = (struct completed_requests*)rc_stack_pop(s->rc_matched_reqs);
    qlist_add(&req->ql, &s->completed_reqs);
  }
}

// Maps an MPI rank to a LP ID
static tw_lpid rank_to_lpid(int rank)
{
  return codes_mapping_get_lpid_from_relative(rank, NULL, "nw-lp", NULL, 0);
}

static int notify_posted_wait(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    unsigned int completed_req)
{
  (void)bf;

  struct pending_waits* wait_elem = s->wait_op;
  int wait_completed = 0;
  m->fwd.wait_completed = 0;

  if (!wait_elem)
    return 0; // No pending waits

  int op_type = wait_elem->op_type;
  if (op_type == CODES_WK_WAIT && (wait_elem->req_ids[0] == completed_req)) {
    // Single wait
    m->fwd.wait_completed = 1;
    wait_completed = 1;
  }
  else if (op_type == CODES_WK_WAITALL || op_type == CODES_WK_WAITANY
      || op_type == CODES_WK_WAITSOME) {
    // Multiple waits
    int i;
    for (i = 0; i < wait_elem->count; i++) {
      if (wait_elem->req_ids[i] == completed_req) {
        wait_elem->num_completed++;
        if (wait_elem->num_completed > wait_elem->count)
          printf("\nNum completed %d count %d LP %llu\n", wait_elem->num_completed,
              wait_elem->count, LLU(lp->gid));

        if (wait_elem->num_completed >= wait_elem->count) {
          if (enable_debug)
            fprintf(debug_log_file, "(time: %lf) APP ID %d MPI WAITALL COMPLETED AT %llu\n",
                tw_now(lp), s->app_id, LLU(s->nw_id));
          wait_completed = 1;
        }

        m->fwd.wait_completed = 1;
      }
    }
  }

  return wait_completed;
}


// Executes MPI_Wait
static void codes_exec_mpi_wait(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    struct codes_workload_op* mpi_op)
{
  assert(!s->wait_op);

  // Check if there is a corresponding operation that is already complete
  int index = 0;
  unsigned int req_id = mpi_op->u.wait.req_id;
  struct completed_requests* current = NULL;
  struct qlist_head* ent = NULL;

  qlist_for_each(ent, &s->completed_reqs) {
    current = qlist_entry(ent, completed_requests, ql);
    if (current->req_id == req_id) {
      bf->c1 = 1;
      qlist_del(&current->ql);
      rc_stack_push(lp, current, free, s->rc_processed_ops);
      issue_next_event(lp);
      m->fwd.found_match = index;
      return;
    }
    ++index;
  }

  // Otherwise, add the wait operation to the queue.
  // This will be later handled by update_completed_queue().
  struct pending_waits* wait_op = (struct pending_waits*)malloc(sizeof(struct pending_waits));
  wait_op->op_type = mpi_op->op_type;
  wait_op->req_ids[0] = req_id;
  wait_op->count = 1;
  wait_op->num_completed = 0;
  wait_op->start_time = tw_now(lp);
  s->wait_op = wait_op;

  return;
}

static void codes_exec_mpi_wait_rc(nw_state* s, tw_bf* bf, tw_lp* lp, nw_message* m)
{
  if (bf->c1) {
    completed_requests* qi = (completed_requests*)rc_stack_pop(s->rc_processed_ops);
    if (m->fwd.found_match == 0) {
      qlist_add(&qi->ql, &s->completed_reqs);
    }
    else {
      int index = 1;
      struct qlist_head* ent = NULL;
      qlist_for_each(ent, &s->completed_reqs) {
        if (index == m->fwd.found_match) {
          qlist_add(&qi->ql, ent);
          break;
        }
        index++;
      }
    }
    issue_next_event_rc(lp);
    return;
  }

  struct pending_waits* wait_op = s->wait_op;
  free(wait_op);
  s->wait_op = NULL;
}

// Executes MPI_Waitall
static void codes_exec_mpi_waitall(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    struct codes_workload_op* mpi_op)
{
  if (enable_debug)
    fprintf(debug_log_file, "MPI WAITALL POSTED FROM %llu\n", LLU(s->nw_id));

  // Handle sampling
  if (enable_sampling) {
    bf->c1 = 1;

    if (tw_now(lp) >= s->cur_interval_end) {
      bf->c2 = 1;
      int indx = s->sampling_indx;
      s->mpi_wkld_samples[indx].nw_id = s->nw_id;
      s->mpi_wkld_samples[indx].app_id = s->app_id;
      s->mpi_wkld_samples[indx].sample_end_time = s->cur_interval_end;
      s->cur_interval_end += sampling_interval;
      s->sampling_indx++;
    }

    if (s->sampling_indx >= MAX_STATS) {
      struct mpi_workload_sample* tmp = (struct mpi_workload_sample*)calloc(
          (MAX_STATS + s->max_arr_size), sizeof(struct mpi_workload_sample));
      memcpy(tmp, s->mpi_wkld_samples, s->sampling_indx);
      free(s->mpi_wkld_samples);
      s->mpi_wkld_samples = tmp;
      s->max_arr_size += MAX_STATS;
    }

    int indx = s->sampling_indx;
    s->mpi_wkld_samples[indx].num_waits_sample++;
  }

  int wait_count = mpi_op->u.waits.count; // Number of requests to wait on
  assert(wait_count < MAX_WAIT_REQS);

  int i = 0, num_matched = 0;
  m->fwd.num_matched = 0;

  // Check number of completed requests that matches to this wait
  for (i = 0; i < wait_count; i++) {
    unsigned int req_id = mpi_op->u.waits.req_ids[i];
    struct qlist_head* ent = NULL;
    struct completed_requests* current = NULL;

    qlist_for_each(ent, &s->completed_reqs) {
      current = qlist_entry(ent, struct completed_requests, ql);
      if (current->req_id == req_id)
        num_matched++;
    }
  }

  m->fwd.found_match = num_matched;

  if (num_matched == wait_count) {
    // All corresponding requests are already complete.
    // Remove all completed requests and fetch next MPI operation
    m->fwd.num_matched = clear_completed_reqs(s, lp, mpi_op->u.waits.req_ids, wait_count);
    struct pending_waits* wait_op = s->wait_op;
    free(wait_op);
    s->wait_op = NULL;

    issue_next_event(lp);
  }
  else {
    // Some requests are not yet complete, add as pending
    struct pending_waits* wait_op = (struct pending_waits*)malloc(sizeof(struct pending_waits));
    wait_op->count = wait_count;
    wait_op->op_type = mpi_op->op_type;

    for (i = 0; i < wait_count; i++)
      wait_op->req_ids[i] =  mpi_op->u.waits.req_ids[i];

    wait_op->num_completed = num_matched;
    wait_op->start_time = tw_now(lp);
    s->wait_op = wait_op;
  }

  return;
}

static void codes_exec_mpi_waitall_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  if (bf->c1) {
    int sampling_indx = s->sampling_indx;
    s->mpi_wkld_samples[sampling_indx].num_waits_sample--;

    if (bf->c2) {
      s->cur_interval_end -= sampling_interval;
      s->sampling_indx--;
    }
  }

  if (s->wait_op) {
    struct pending_waits* wait_op = s->wait_op;
    free(wait_op);
    s->wait_op = NULL;
  }
  else {
    add_completed_reqs(s, lp, m->fwd.num_matched);
    issue_next_event_rc(lp);
  }

  return;
}

static int remove_matching_send(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    mpi_msg_queue* qitem)
{
  int matched = 0;
  int index = 0;
  int is_rend = 0;
  struct qlist_head* ent = NULL;
  mpi_msg_queue* qi = NULL;

  // Search the queue for a match
  qlist_for_each(ent, &s->arrival_queue) {
    qi = qlist_entry(ent, mpi_msg_queue, ql);
    // Send and receive sizes don't have to match in MPI
    if ((qi->tag == qitem->tag || qitem->tag == -1)
        && ((qi->source_rank == qitem->source_rank) || qitem->source_rank == -1)) {
      qitem->num_bytes = qi->num_bytes; // XXX: This is different from remove_matching_recv
      matched = 1;
      break;
    }
    ++index;
  }

  if (matched) {
    if (enable_msg_tracking && (qi->num_bytes < eager_limit)) {
      update_message_size(s, lp, bf, m, qi, 1, 0);
    }

    m->fwd.matched_req = qitem->req_id;
    if (qitem->num_bytes >= eager_limit) {
      // Matching message arrival (send) found.
      // Need to notify sender to transmit the actual data.
      bf->c10 = 1;
      is_rend = 1;
      send_ack_back(s, bf, m, lp, qi, qitem->req_id);
    }

    // XXX: Why isn't this only set with eager messages like remove_matching_recv?
    m->rc.saved_recv_time = s->recv_time;
    m->rc.saved_recv_time_sample = s->ross_sample.recv_time;
    s->recv_time += (tw_now(lp) - qitem->req_init_time);
    s->ross_sample.recv_time += (tw_now(lp) - qitem->req_init_time);

    if (qitem->op_type == CODES_WK_IRECV && !is_rend) {
      // Irecv complete, update queue
      bf->c29 = 1;
      update_completed_queue(s, bf, m, lp, qitem->req_id);
    }
    else if (qitem->op_type == CODES_WK_RECV && !is_rend) {
      // Recv complete, proceed to next operation
      bf->c6 = 1;
      issue_next_event(lp);
    }

    qlist_del(&qi->ql); // Remove matched send (arrival)

    rc_stack_push(lp, qi, free, s->rc_processed_ops);

    return index;
  }

  return -1;
}

// Trigger to fetch next MPI operation
static void issue_next_event(tw_lp* lp)
{
  tw_stime ts = g_tw_lookahead + 0.1 + tw_rand_exponential(lp->rng, noise);
  assert(ts > 0);

  tw_event* e = tw_event_new(lp->gid, ts, lp);
  nw_message* msg = (nw_message*)tw_event_data(e);
  msg->msg_type = MPI_OP_GET_NEXT;

  tw_event_send(e);
}

static void issue_next_event_rc(tw_lp* lp)
{
  tw_rand_reverse_unif(lp->rng);
}

// Simulate computation/delay between MPI operations
static void codes_exec_comp_delay(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    struct codes_workload_op* mpi_op)
{
  bf->c28 = 0;
  tw_event* e;
  tw_stime ts;
  nw_message* msg;

  m->rc.saved_delay = s->compute_time;
  m->rc.saved_delay_sample = s->ross_sample.compute_time;
  s->compute_time += (mpi_op->u.delay.nsecs/compute_time_speedup);
  s->ross_sample.compute_time += (mpi_op->u.delay.nsecs/compute_time_speedup);

  ts = (mpi_op->u.delay.nsecs/compute_time_speedup);
  if (ts <= g_tw_lookahead) {
    bf->c28 = 1;
    ts = g_tw_lookahead + 0.1 + tw_rand_exponential(lp->rng, noise);
  }

  assert(ts > 0);

  e = tw_event_new(lp->gid, ts , lp);
  msg = (nw_message*)tw_event_data(e);
  msg->msg_type = MPI_OP_GET_NEXT;
  tw_event_send(e);
}

static void codes_exec_comp_delay_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  if (bf->c28)
    tw_rand_reverse_unif(lp->rng);

  s->compute_time = m->rc.saved_delay;
  s->ross_sample.compute_time = m->rc.saved_delay_sample;
}

// Executes MPI_Recv and MPI_Irecv operations
static void codes_exec_mpi_recv(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    struct codes_workload_op* mpi_op)
{
  // Save information for RC
  m->rc.saved_recv_time = s->recv_time;
  m->rc.saved_recv_time_sample = s->ross_sample.recv_time;
  m->rc.saved_num_bytes = mpi_op->u.recv.num_bytes;

  // Create entry for MPI recv operation.
  // This is used to find if there is a matching (arrived) send;
  // if not it is stored in pending_recvs_queue for matching later.
  mpi_msg_queue* recv_op = (mpi_msg_queue*) malloc(sizeof(mpi_msg_queue));
  recv_op->op_type = mpi_op->op_type;
  recv_op->tag = mpi_op->u.recv.tag;
  recv_op->source_rank = mpi_op->u.recv.source_rank;
  recv_op->dest_rank = mpi_op->u.recv.dest_rank;
  recv_op->num_bytes = mpi_op->u.recv.num_bytes;
  recv_op->req_init_time = tw_now(lp);
  recv_op->req_id = mpi_op->u.recv.req_id;

  int found_matching_send = remove_matching_send(s, bf, m, lp, recv_op);

  if (mpi_op->op_type == CODES_WK_IRECV) {
    bf->c6 = 1;
    issue_next_event(lp);
  }

  if (found_matching_send < 0) {
    m->fwd.found_match = -1;
    qlist_add_tail(&recv_op->ql, &s->pending_recvs_queue);
  }
  else {
    m->fwd.found_match = found_matching_send; // Record element index for RC
  }
}

static void codes_exec_mpi_recv_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  s->recv_time = m->rc.saved_recv_time;
  s->ross_sample.recv_time = m->rc.saved_recv_time_sample;

  if (bf->c6)
    issue_next_event_rc(lp);

  if (m->fwd.found_match >= 0) {
    s->recv_time = m->rc.saved_recv_time;
    s->ross_sample.recv_time = m->rc.saved_recv_time_sample;

    mpi_msg_queue* qi = (mpi_msg_queue*)rc_stack_pop(s->rc_processed_ops);

    if (bf->c10)
      send_ack_back_rc(s, bf, m, lp);

    if (m->fwd.found_match == 0) {
      qlist_add(&qi->ql, &s->arrival_queue);
    }
    else {
      int index = 1;
      struct qlist_head* ent = NULL;
      qlist_for_each(ent, &s->arrival_queue) {
        if (index == m->fwd.found_match) {
          qlist_add(&qi->ql, ent);
          break;
        }
        index++;
      }
    }

    if (bf->c29) {
      update_completed_queue_rc(s, bf, m, lp);
    }
  }
  else if (m->fwd.found_match < 0) {
    struct qlist_head* ent = qlist_pop_back(&s->pending_recvs_queue);
    mpi_msg_queue* qi = qlist_entry(ent, mpi_msg_queue, ql);
    free(qi);
  }
}

int get_global_id_of_job_rank(tw_lpid job_rank, int app_id)
{
    struct codes_jobmap_id lid;
    lid.job = app_id;
    lid.rank = job_rank;
    int global_rank = codes_jobmap_to_global_id(lid, jobmap_ctx);
    return global_rank;
}

// Executes MPI_Send and MPI_Isend operations
static void codes_exec_mpi_send(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    struct codes_workload_op* mpi_op, int is_rend)
{
  bf->c3 = 0;
  bf->c1 = 0;
  bf->c4 = 0;

  // Determine priority
  char prio[12];
  if (priority_type == 0) {
    if (s->app_id == 0)
      strcpy(prio, "high");
    else
      strcpy(prio, "medium");
  }
  else if (priority_type == 1) {
    if (mpi_op->u.send.tag == COL_TAG || mpi_op->u.send.tag == BAR_TAG)
      strcpy(prio, "high");
    else
      strcpy(prio, "medium");
  }
  else
    tw_error(TW_LOC, "\nInvalid priority type %d\n", priority_type);

  // Figure out destination ranks
  int global_dest_rank = mpi_op->u.send.dest_rank;
  if (alloc_spec) {
    global_dest_rank = get_global_id_of_job_rank(mpi_op->u.send.dest_rank, s->app_id);
  }

  tw_lpid dest_rank = codes_mapping_get_lpid_from_relative(global_dest_rank, NULL, "nw-lp", NULL, 0);

  // Save message size for RC
  m->rc.saved_num_bytes = mpi_op->u.send.num_bytes;

  // Handle sampling
  if (enable_sampling) {
    if (tw_now(lp) >= s->cur_interval_end) {
      bf->c1 = 1;
      int indx = s->sampling_indx;
      s->mpi_wkld_samples[indx].nw_id = s->nw_id;
      s->mpi_wkld_samples[indx].app_id = s->app_id;
      s->mpi_wkld_samples[indx].sample_end_time = s->cur_interval_end;
      s->sampling_indx++;
      s->cur_interval_end += sampling_interval;
    }
    if (s->sampling_indx >= MAX_STATS) {
      struct mpi_workload_sample * tmp = (struct mpi_workload_sample*)calloc(
          (MAX_STATS + s->max_arr_size), sizeof(struct mpi_workload_sample));
      memcpy(tmp, s->mpi_wkld_samples, s->sampling_indx);
      free(s->mpi_wkld_samples);
      s->mpi_wkld_samples = tmp;
      s->max_arr_size += MAX_STATS;
    }
    int indx = s->sampling_indx;
    s->mpi_wkld_samples[indx].num_sends_sample++;
    s->mpi_wkld_samples[indx].num_bytes_sample += mpi_op->u.send.num_bytes;
  }

  nw_message local_m;
  nw_message remote_m;

  // Construct local message
  local_m.msg_type = MPI_SEND_POSTED;
  local_m.op_type = mpi_op->op_type;
  local_m.fwd.src_rank = mpi_op->u.send.source_rank;
  local_m.fwd.dest_rank = mpi_op->u.send.dest_rank;
  local_m.fwd.num_bytes = mpi_op->u.send.num_bytes;
  local_m.fwd.req_id = mpi_op->u.send.req_id;
  local_m.fwd.matched_req = m->fwd.matched_req;
  local_m.fwd.tag = mpi_op->u.send.tag;
  local_m.fwd.app_id = s->app_id;
  local_m.fwd.rend_send = 0;

  int is_eager = 0;
  if (mpi_op->u.send.num_bytes < eager_limit) {
    // Eager message, directly issue a model-net send
    bf->c15 = 1;
    is_eager = 1;
    s->num_sends++;
    s->ross_sample.num_sends++;

    local_m.fwd.sim_start_time = tw_now(lp);
    remote_m = local_m;
    remote_m.msg_type = MPI_SEND_ARRIVED;

    tw_stime copy_overhead = copy_per_byte_eager * mpi_op->u.send.num_bytes;

    // Pass to model-net
    m->event_rc = model_net_event_mctx(net_id, &mapping_context, &mapping_context,
        prio, dest_rank, mpi_op->u.send.num_bytes, (self_overhead + copy_overhead + soft_delay_mpi + nic_delay),
        sizeof(nw_message), (const void*)&remote_m, sizeof(nw_message), (const void*)&local_m, lp);
  }
  else if (is_rend == 0) {
    // Rendezvous message, need to initiate handshake
    // Issue a control message to the destination, with only remote message
    bf->c16 = 1;
    s->num_sends++;
    s->ross_sample.num_sends++;

    remote_m.msg_type = MPI_SEND_ARRIVED;
    remote_m.op_type = mpi_op->op_type;
    remote_m.fwd.src_rank = mpi_op->u.send.source_rank;
    remote_m.fwd.dest_rank = mpi_op->u.send.dest_rank;
    remote_m.fwd.num_bytes = mpi_op->u.send.num_bytes;
    remote_m.fwd.sim_start_time = tw_now(lp);
    remote_m.fwd.req_id = mpi_op->u.send.req_id;
    remote_m.fwd.tag = mpi_op->u.send.tag;
    remote_m.fwd.app_id = s->app_id;

    // Pass control message to model-net
    m->event_rc = model_net_event_mctx(net_id, &mapping_context, &mapping_context,
        prio, dest_rank, CONTROL_MSG_SZ, (self_overhead + soft_delay_mpi + nic_delay),
        sizeof(nw_message), (const void*)&remote_m, 0, NULL, lp);
  }
  else if (is_rend == 1) {
    // Rendezvous message, need to send actual data (ACK received)
    bf->c17 = 1;

    local_m.fwd.sim_start_time = mpi_op->sim_start_time;
    local_m.fwd.rend_send = 1;
    remote_m = local_m;
    remote_m.msg_type = MPI_REND_ARRIVED;

    // Pass to model-net
    m->event_rc = model_net_event_mctx(net_id, &mapping_context, &mapping_context,
        prio, dest_rank, mpi_op->u.send.num_bytes, (self_overhead + soft_delay_mpi + nic_delay),
        sizeof(nw_message), (const void*)&remote_m, sizeof(nw_message), (const void*)&local_m, lp);
  }

  // Debugging output (only at initiation for rendezvous messages)
  if (enable_debug && !is_rend) {
    if (mpi_op->op_type == CODES_WK_ISEND)
      fprintf(debug_log_file, "(time: %lf) APP %d MPI ISEND SOURCE %llu DEST %d TAG %d BYTES %"PRId64,
          tw_now(lp), s->app_id, LLU(s->nw_id), global_dest_rank, mpi_op->u.send.tag, mpi_op->u.send.num_bytes);
    else
      fprintf(debug_log_file, "(time: %lf) APP ID %d MPI SEND SOURCE %llu DEST %d TAG %d BYTES %"PRId64,
          tw_now(lp), s->app_id, LLU(s->nw_id), global_dest_rank, mpi_op->u.send.tag, mpi_op->u.send.num_bytes);
  }

  // Record message size for sends with actual data
  if (is_rend || is_eager) {
    bf->c3 = 1;
    s->num_bytes_sent += mpi_op->u.send.num_bytes;
    s->ross_sample.num_bytes_sent += mpi_op->u.send.num_bytes;
    num_bytes_sent += mpi_op->u.send.num_bytes;
  }

  // For non-blocking send, get next MPI operation
  if (mpi_op->op_type == CODES_WK_ISEND && (!is_rend || is_eager)) {
    bf->c4 = 1;
    issue_next_event(lp);
  }
}

static void codes_exec_mpi_send_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  // Handle sampling
  if (enable_sampling) {
    int indx = s->sampling_indx;
    s->mpi_wkld_samples[indx].num_sends_sample--;
    s->mpi_wkld_samples[indx].num_bytes_sample -= m->rc.saved_num_bytes;

    if (bf->c1) {
      s->sampling_indx--;
      s->cur_interval_end -= sampling_interval;
    }
  }

  // Eager or rendezvous control message
  if (bf->c15 || bf->c16) {
    s->num_sends--;
    s->ross_sample.num_sends--;
  }

  // Reverse model-net events
  if (bf->c15) model_net_event_rc2(lp, &m->event_rc);
  if (bf->c16) model_net_event_rc2(lp, &m->event_rc);
  if (bf->c17) model_net_event_rc2(lp, &m->event_rc);

  // Reverse fetching next MPI operation
  if (bf->c4) issue_next_event_rc(lp);

  // Reverse message size recording
  if (bf->c3) {
    s->num_bytes_sent -= m->rc.saved_num_bytes;
    s->ross_sample.num_bytes_sent -= m->rc.saved_num_bytes;
    num_bytes_sent -= m->rc.saved_num_bytes;
  }
}

// Notify posted waits or proceed to next event
static void update_completed_queue(nw_state* s, tw_bf* bf, nw_message* m,
    tw_lp* lp, dumpi_req_id req_id)
{
  bf->c30 = 0;
  bf->c31 = 0;
  m->fwd.num_matched = 0;

  int waiting = notify_posted_wait(s, bf, m, lp, req_id);

  if (!waiting) {
    // No matching wait found or only partially fulfilled (e.g. MPI_Waitall)
    bf->c30 = 1;
    completed_requests* req = (completed_requests*)malloc(sizeof(completed_requests));
    req->req_id = req_id;
    qlist_add(&req->ql, &s->completed_reqs);
  }
  else {
    // Wait fulfilled, clear corresponding completed requests
    bf->c31 = 1;
    m->fwd.num_matched = clear_completed_reqs(s, lp, s->wait_op->req_ids, s->wait_op->count);

    m->rc.saved_wait_time = s->wait_time;
    m->rc.saved_wait_time_sample = s->ross_sample.wait_time;
    s->wait_time += (tw_now(lp) - s->wait_op->start_time);
    s->ross_sample.wait_time += (tw_now(lp) - s->wait_op->start_time);

    struct pending_waits* wait_elem = s->wait_op;
    rc_stack_push(lp, wait_elem, free, s->rc_processed_wait_op);
    s->wait_op = NULL;

    // Go on to next operation
    issue_next_event(lp);
  }
}

static void update_completed_queue_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  if (bf->c30) {
    // Wait was not fulfilled
    struct qlist_head* ent = qlist_pop(&s->completed_reqs);
    completed_requests* req = qlist_entry(ent, completed_requests, ql);
    free(req);
  }
  else if (bf->c31) {
    // Wait was fulfilled
    struct pending_waits* wait_elem = (struct pending_waits*)rc_stack_pop(s->rc_processed_wait_op);
    s->wait_op = wait_elem;

    s->wait_time = m->rc.saved_wait_time;
    s->ross_sample.wait_time = m->rc.saved_wait_time_sample;

    add_completed_reqs(s, lp, m->fwd.num_matched);

    issue_next_event_rc(lp);
  }

  if (m->fwd.wait_completed > 0)
    s->wait_op->num_completed--;
}

// Send ACK back to sender for rendezvous messages
static void send_ack_back(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    mpi_msg_queue* mpi_op, int matched_req)
{
  (void)bf;

  // Figure out destination rank (sender)
  int global_dest_rank = mpi_op->source_rank;
  if (alloc_spec)
    global_dest_rank =  get_global_id_of_job_rank(mpi_op->source_rank, s->app_id);
  tw_lpid dest_rank = codes_mapping_get_lpid_from_relative(global_dest_rank, NULL, "nw-lp", NULL, 0);

  // Build remote message
  nw_message remote_m;
  remote_m.op_type = mpi_op->op_type;
  remote_m.msg_type = MPI_REND_ACK_ARRIVED;
  remote_m.fwd.src_rank = mpi_op->source_rank;
  remote_m.fwd.dest_rank = mpi_op->dest_rank;
  remote_m.fwd.num_bytes = mpi_op->num_bytes;
  remote_m.fwd.sim_start_time = mpi_op->req_init_time;
  remote_m.fwd.req_id = mpi_op->req_id;
  remote_m.fwd.matched_req = matched_req;
  remote_m.fwd.tag = mpi_op->tag;

  // Determine priority
  char prio[12];
  if (priority_type == 0) {
    if (s->app_id == 0) strcpy(prio, "high");
    else if (s->app_id == 1) strcpy(prio, "medium");
  }
  else if (priority_type == 1) {
    if (mpi_op->tag == COL_TAG || mpi_op->tag == BAR_TAG) strcpy(prio, "high");
    else strcpy(prio, "medium");
  }
  else
    tw_error(TW_LOC, "\nInvalid app id\n");

  // Send ACK via model-net
  m->event_rc = model_net_event_mctx(net_id, &mapping_context, &mapping_context,
      prio, dest_rank, CONTROL_MSG_SZ, (self_overhead + soft_delay_mpi + nic_delay),
      sizeof(nw_message), (const void*)&remote_m, 0, NULL, lp);
}

static void send_ack_back_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  (void)s;
  (void)bf;

  model_net_event_rc2(lp, &m->event_rc);
}

// Search for a matching MPI receive operation and remove it from the list.
// The index of the removed element in the list is recorded, which is used for
// inserting it again during reverse computation.
static int remove_matching_recv(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp,
    mpi_msg_queue* qitem)
{
  int matched = 0;
  int index = 0;
  int is_rend = 0;
  struct qlist_head* ent = NULL;
  mpi_msg_queue* qi = NULL;

  // Search the queue for a match
  qlist_for_each(ent, &s->pending_recvs_queue) {
    qi = qlist_entry(ent, mpi_msg_queue, ql);
    // Send and receive sizes don't have to match in MPI
    if (((qi->tag == qitem->tag) || qi->tag == -1)
        && ((qi->source_rank == qitem->source_rank) || qi->source_rank == -1)) {
      matched = 1;
      qi->num_bytes = qitem->num_bytes;
      break;
    }
    ++index;
  }

  if (matched) {
    if (qitem->num_bytes < eager_limit) {
      // Eager message, store message information
      if (enable_msg_tracking) {
        update_message_size(s, lp, bf, m, qitem, 1, 1);
      }

      bf->c12 = 1;
      m->rc.saved_recv_time = s->recv_time;
      m->rc.saved_recv_time_sample = s->ross_sample.recv_time;
      s->recv_time += (tw_now(lp) - m->fwd.sim_start_time);
      s->ross_sample.recv_time += (tw_now(lp) - m->fwd.sim_start_time);
    }
    else {
      // Matching receive found for rendezvous control message.
      // Need to notify sender to transmit the actual data.
      // XXX: Only works in sequential mode?
      bf->c10 = 1;
      is_rend = 1;
      send_ack_back(s, bf, m, lp, qitem, qi->req_id);
    }

    if (qi->op_type == CODES_WK_IRECV && !is_rend) {
      // Irecv complete, update queue
      bf->c9 = 1;
      update_completed_queue(s, bf, m, lp, qi->req_id);
    }
    else if (qi->op_type == CODES_WK_RECV && !is_rend) {
      // Recv complete, proceed to next operation
      bf->c8 = 1;
      issue_next_event(lp);
    }

    qlist_del(&qi->ql); // Remove matched recv

    rc_stack_push(lp, qi, free, s->rc_processed_ops);

    return index;
  }

  return -1;
}

// Received an eager message or a control message for rendezvous
static void update_arrival_queue(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  // Check if message is from the same application
  if (s->app_id != m->fwd.app_id)
    printf("\nReceived message for app %d but my app is %d! My rank is %llu\n",
        m->fwd.app_id, s->app_id, LLU(s->nw_id));
  assert(s->app_id == m->fwd.app_id);

  m->rc.saved_recv_time = s->recv_time;
  m->rc.saved_recv_time_sample = s->ross_sample.recv_time;

  s->num_bytes_recvd += m->fwd.num_bytes;
  s->ross_sample.num_bytes_recvd += m->fwd.num_bytes;
  num_bytes_recvd += m->fwd.num_bytes;

  int global_src_id = m->fwd.src_rank;
  if (alloc_spec) {
    global_src_id = get_global_id_of_job_rank(m->fwd.src_rank, s->app_id);
  }

  if (m->fwd.num_bytes < eager_limit) {
    // If received message is eager, send a callback with message latency
    bf->c1 = 1;
    tw_stime ts = codes_local_latency(lp);
    assert(ts > 0);
    tw_event *e_callback = tw_event_new(rank_to_lpid(global_src_id), ts, lp);
    nw_message *m_callback = (nw_message*)tw_event_data(e_callback);
    m_callback->msg_type = MPI_SEND_ARRIVED_CB;
    m_callback->fwd.msg_send_time = tw_now(lp) - m->fwd.sim_start_time;
    tw_event_send(e_callback);
  }

  // Create entry for arrived MPI send operation.
  // This is used to find if there is a matching receive;
  // if not it is stored in arriavl_queue for matching later.
  mpi_msg_queue* arrived_op = (mpi_msg_queue*) malloc(sizeof(mpi_msg_queue));
  arrived_op->op_type = m->op_type;
  arrived_op->tag = m->fwd.tag;
  arrived_op->source_rank = m->fwd.src_rank;
  arrived_op->dest_rank = m->fwd.dest_rank;
  arrived_op->num_bytes = m->fwd.num_bytes;
  arrived_op->req_init_time = m->fwd.sim_start_time;
  arrived_op->req_id = m->fwd.req_id;

  int found_matching_recv = remove_matching_recv(s, bf, m, lp, arrived_op);

  if (found_matching_recv < 0) {
    m->fwd.found_match = -1;
    qlist_add_tail(&arrived_op->ql, &s->arrival_queue);
  }
  else {
    m->fwd.found_match = found_matching_recv; // Record element index for RC
    free(arrived_op);
  }
}

static void update_arrival_queue_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  s->num_bytes_recvd -= m->fwd.num_bytes;
  s->ross_sample.num_bytes_recvd -= m->fwd.num_bytes;
  num_bytes_recvd -= m->fwd.num_bytes;

  if (bf->c1)
    codes_local_latency_reverse(lp);

  if (bf->c10)
    send_ack_back_rc(s, bf, m, lp);

  // Matching recv was found
  if (m->fwd.found_match >= 0) {
    mpi_msg_queue* qi = (mpi_msg_queue*)rc_stack_pop(s->rc_processed_ops);

    // Insert recv back to the right place at the queue
    if (m->fwd.found_match == 0) {
      qlist_add(&qi->ql, &s->pending_recvs_queue);
    }
    else {
      int index = 1;
      struct qlist_head* ent = NULL;
      qlist_for_each(ent, &s->pending_recvs_queue) {
        if (index == m->fwd.found_match) {
          qlist_add(&qi->ql, ent);
          break;
        }
        index++;
      }
    }

    if (bf->c12) {
      s->recv_time = m->rc.saved_recv_time;
      s->ross_sample.recv_time = m->rc.saved_recv_time_sample;
    }

    if (bf->c9)
      update_completed_queue_rc(s, bf, m, lp);

    if (bf->c8)
      issue_next_event_rc(lp);
  }
  else {
    struct qlist_head* ent = qlist_pop_back(&s->arrival_queue);
    mpi_msg_queue* qi = qlist_entry(ent, mpi_msg_queue, ql);
    free(qi);
  }
}

static void update_message_time(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  (void)bf;
  (void)lp;

  m->rc.saved_send_time = s->send_time;
  m->rc.saved_send_time_sample = s->ross_sample.send_time;
  s->send_time += m->fwd.msg_send_time;
  s->ross_sample.send_time += m->fwd.msg_send_time;
}

static void update_message_time_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  (void)bf;
  (void)lp;
  s->send_time = m->rc.saved_send_time;
  s->ross_sample.send_time = m->rc.saved_send_time_sample;
}

void nw_test_init(nw_state* s, tw_lp* lp)
{
  // Initialize LP state
  memset(s, 0, sizeof(*s));
  s->nw_id = codes_mapping_get_lp_relative_id(lp->gid, 0, 0);
  s->mpi_wkld_samples = (struct mpi_workload_sample*)calloc(MAX_STATS,
      sizeof(struct mpi_workload_sample));
  s->sampling_indx = 0;
  s->is_finished = 0;
  s->cur_interval_end = 0;
  s->col_time = 0;
  s->num_reduce = 0;
  s->reduce_time = 0;
  s->all_reduce_time = 0;
  s->max_time = 0;

  char type_name[512];

  // Confirm that there is at least one trace, and
  // number of traces are <= number of simulated MPI ranks (LPs)
  assert(num_net_traces > 0 && num_net_traces <= total_ranks);

  struct codes_jobmap_id lid;
  if (alloc_spec) {
    // Job allocation specified by input file
    lid = codes_jobmap_to_local_id(s->nw_id, jobmap_ctx);
    if (lid.job == -1) {
      s->app_id = -1;
      s->local_rank = -1;
      return;
    }
  }
  else {
    // Only one job running with contiguous mapping
    lid.job = 0;
    lid.rank = s->nw_id;
    s->app_id = 0;

    // Check bounds
    if ((int)s->nw_id >= num_net_traces)
      return;
  }

  if (strcmp(workload_type, "dumpi") == 0) {
    // Using DUMPI traces
    dumpi_trace_params params_d;
    strcpy(params_d.file_name, file_name_of_job[lid.job]);
    params_d.num_net_traces = num_traces_of_job[lid.job];
    params_d.nprocs = nprocs;
    params = (char*)&params_d;
    strcpy(type_name, "dumpi-trace-workload");
#ifdef ENABLE_CORTEX_PYTHON
    strcpy(params_d.cortex_script, cortex_file);
    strcpy(params_d.cortex_class, cortex_class);
    strcpy(params_d.cortex_gen, cortex_gen);
#endif
  }

  // Initialize queues
  INIT_QLIST_HEAD(&s->arrival_queue);
  INIT_QLIST_HEAD(&s->pending_recvs_queue);
  INIT_QLIST_HEAD(&s->completed_reqs);

  // Create data structures to keep track of messages if tracking is enabled
  if (enable_msg_tracking) {
    INIT_QLIST_HEAD(&s->msg_sz_list);
    s->msg_sz_table = qhash_init(msg_size_hash_compare, quickhash_64bit_hash, RANK_HASH_TABLE_SZ);
    assert(s->msg_sz_table != NULL);
  }

  // Initialize stacks for reverse computation
  rc_stack_create(&s->rc_processed_ops);
  rc_stack_create(&s->rc_processed_wait_op);
  rc_stack_create(&s->rc_matched_reqs);

  assert(s->rc_processed_ops != NULL);
  assert(s->rc_processed_wait_op != NULL);
  assert(s->rc_matched_reqs != NULL);

  s->start_time = tw_now(lp); // Start clock with first event
  s->num_bytes_sent = 0;
  s->num_bytes_recvd = 0;
  s->compute_time = 0;
  s->elapsed_time = 0;

  s->app_id = lid.job;
  s->local_rank = lid.rank;

  // Load the workload into CODES.
  // Workload ID is used to fetch the next MPI operation.
  wrkld_id = codes_workload_load(type_name, params, s->app_id, s->local_rank);

  // Create event to trigger next MPI operation
  issue_next_event(lp);

  // Initialize sampling and create metadata file
  if (enable_sampling && sampling_interval > 0) {
    s->max_arr_size = MAX_STATS;
    s->cur_interval_end = sampling_interval;
    if (!g_tw_mynode && !s->nw_id) {
      fprintf(meta_log_file, "mpi_proc_id app_id num_waits num_sends num_bytes_sent sample_end_time");
    }
  }

  return;
}

void nw_test_event_handler(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  assert(s->app_id >= 0 && s->local_rank >= 0);

  // Garbage collection for RCs with gen time < GVT
  rc_stack_gc(lp, s->rc_matched_reqs);
  rc_stack_gc(lp, s->rc_processed_ops);
  rc_stack_gc(lp, s->rc_processed_wait_op);

  switch (m->msg_type) {
    case MPI_SEND_ARRIVED:
      update_arrival_queue(s, bf, m, lp);
      break;
    case MPI_REND_ARRIVED:
      {
        if (enable_msg_tracking) {
          mpi_msg_queue mpi_op;
          mpi_op.op_type = m->op_type;
          mpi_op.tag = m->fwd.tag;
          mpi_op.num_bytes = m->fwd.num_bytes;
          mpi_op.source_rank = m->fwd.src_rank;
          mpi_op.dest_rank = m->fwd.dest_rank;
          mpi_op.req_init_time = m->fwd.sim_start_time;

          update_message_size(s, lp, bf, m, &mpi_op, 0, 1);
        }

        int global_src_id = m->fwd.src_rank;
        if (alloc_spec)
          global_src_id = get_global_id_of_job_rank(m->fwd.src_rank, s->app_id);

        // Create callback event to log message time
        tw_event *e_callback = tw_event_new(rank_to_lpid(global_src_id),
            codes_local_latency(lp), lp);
        nw_message *m_callback = (nw_message*)tw_event_data(e_callback);
        m_callback->msg_type = MPI_SEND_ARRIVED_CB;
        m_callback->fwd.msg_send_time = tw_now(lp) - m->fwd.sim_start_time;
        tw_event_send(e_callback);

        if (m->fwd.matched_req >= 0) { // Request ID pending completion
          bf->c8 = 1;
          update_completed_queue(s, bf, m, lp, m->fwd.matched_req);
        }
        else { // Blocking receive pending completion
          bf->c10 = 1;
          issue_next_event(lp);
        }

        m->rc.saved_recv_time = s->recv_time;
        m->rc.saved_recv_time_sample = s->ross_sample.recv_time;
        s->recv_time += (tw_now(lp) - m->fwd.sim_start_time);
        s->ross_sample.recv_time += (tw_now(lp) - m->fwd.sim_start_time);
      }
      break;
    case MPI_REND_ACK_ARRIVED:
      {
        // Reconstruct the op and pass it on for actual data transfer
        struct codes_workload_op mpi_op;
        mpi_op.op_type = m->op_type;
        mpi_op.u.send.tag = m->fwd.tag;
        mpi_op.u.send.num_bytes = m->fwd.num_bytes;
        mpi_op.u.send.source_rank = m->fwd.src_rank;
        mpi_op.u.send.dest_rank = m->fwd.dest_rank;
        mpi_op.sim_start_time = m->fwd.sim_start_time;
        mpi_op.u.send.req_id = m->fwd.req_id;

        codes_exec_mpi_send(s, bf, m, lp, &mpi_op, 1);
      }
      break;
    case MPI_SEND_ARRIVED_CB:
      update_message_time(s, bf, m, lp);
      break;
    case MPI_SEND_POSTED:
      {
        // Check if message is eager
        int is_eager = 0;
        if (m->fwd.num_bytes < eager_limit)
          is_eager = 1;

        if (m->op_type == CODES_WK_SEND && (is_eager == 1 || m->fwd.rend_send == 1)) {
          bf->c29 = 1;
          issue_next_event(lp);
        }
        else {
          if (m->op_type == CODES_WK_ISEND && (is_eager == 1 || m->fwd.rend_send == 1)) {
            bf->c28 = 1;
            update_completed_queue(s, bf, m, lp, m->fwd.req_id);
          }
        }
      }
      break;
    case MPI_OP_GET_NEXT:
      get_next_mpi_operation(s, bf, m, lp);
      break;
  }
}

void nw_test_event_handler_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  switch (m->msg_type) {
    case MPI_SEND_ARRIVED:
      update_arrival_queue_rc(s, bf, m, lp);
      break;
    case MPI_SEND_ARRIVED_CB:
      update_message_time_rc(s, bf, m, lp);
      break;
    case MPI_SEND_POSTED:
      {
        if (bf->c29)
          issue_next_event_rc(lp);
        if (bf->c28)
          update_completed_queue_rc(s, bf, m, lp);
      }
      break;
    case MPI_REND_ACK_ARRIVED:
      {
        codes_exec_mpi_send_rc(s, bf, m, lp);
      }
      break;
    case MPI_REND_ARRIVED:
      {
        codes_local_latency_reverse(lp);

        if (bf->c10)
          issue_next_event_rc(lp);

        if (bf->c8)
          update_completed_queue_rc(s, bf, m, lp);

        s->recv_time = m->rc.saved_recv_time;
        s->ross_sample.recv_time = m->rc.saved_recv_time_sample;
      }
      break;
    case MPI_OP_GET_NEXT:
      get_next_mpi_operation_rc(s, bf, m, lp);
      break;
  }
}

static void get_next_mpi_operation(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp) {
  struct codes_workload_op* mpi_op = (struct codes_workload_op*)malloc(sizeof(struct codes_workload_op));
  codes_workload_get_next(wrkld_id, s->app_id, s->local_rank, mpi_op);
  m->mpi_op = mpi_op;
  m->op_type = mpi_op->op_type;

  if (mpi_op->op_type == CODES_WK_END) {
    // No more workload
    s->elapsed_time = tw_now(lp) - s->start_time;
    s->is_finished = 1;

    if (!alloc_spec) {
      bf->c9 = 1;
      return;
    }

    printf("[nw-lp %d] Rank %d of app %d finished at %lf\n", s->nw_id, s->local_rank, s->app_id, tw_now(lp));

    return;
  }

  switch (mpi_op->op_type) {
    case CODES_WK_SEND:
    case CODES_WK_ISEND:
      {
        codes_exec_mpi_send(s, bf, m, lp, mpi_op, 0);
      }
      break;
    case CODES_WK_RECV:
    case CODES_WK_IRECV:
      {
        s->num_recvs++;
        s->ross_sample.num_recvs++;
        codes_exec_mpi_recv(s, bf, m, lp, mpi_op);
      }
      break;
    case CODES_WK_DELAY:
      {
        s->num_delays++;
        // If compute simulation is disabled, fetch the next MPI operation
        if (disable_compute)
          issue_next_event(lp);
        else
          codes_exec_comp_delay(s, bf, m, lp, mpi_op);
      }
      break;
    case CODES_WK_WAITSOME:
    case CODES_WK_WAITANY:
      {
        s->num_waitsome++;
        issue_next_event(lp);
      }
      break;
    case CODES_WK_WAITALL:
      {
        s->num_waitall++;
        codes_exec_mpi_waitall(s, bf, m, lp, mpi_op);
        //issue_next_event(lp);
      }
      break;
    case CODES_WK_WAIT:
      {
        s->num_wait++;
        codes_exec_mpi_wait(s, bf, m, lp, mpi_op);
      }
      break;
    case CODES_WK_ALLREDUCE:
      {
        s->num_cols++;
        if (s->col_time > 0) {
          bf->c27 = 1;
          m->rc.saved_delay = s->all_reduce_time;
          s->all_reduce_time += (tw_now(lp) - s->col_time);
          m->rc.saved_send_time = s->col_time;
          s->col_time = 0;
          s->num_all_reduce++;
        }
        else {
          s->col_time = tw_now(lp);
        }
        issue_next_event(lp);
      }
      break;
    case CODES_WK_REDUCE:
    case CODES_WK_BCAST:
    case CODES_WK_ALLGATHER:
    case CODES_WK_ALLGATHERV:
    case CODES_WK_ALLTOALL:
    case CODES_WK_ALLTOALLV:
    case CODES_WK_COL:
      {
        s->num_cols++;
        issue_next_event(lp);
      }
      break;
    default:
      printf("Invalid op type %d\n", mpi_op->op_type);
  }
  return;
}

static void get_next_mpi_operation_rc(nw_state* s, tw_bf* bf, nw_message* m, tw_lp* lp)
{
  codes_workload_get_next_rc(wrkld_id, s->app_id, s->local_rank, m->mpi_op);

  if (m->op_type == CODES_WK_END) {
    s->is_finished = 0;
    return;
  }

  switch (m->op_type) {
    case CODES_WK_SEND:
    case CODES_WK_ISEND:
      {
        codes_exec_mpi_send_rc(s, bf, m, lp);
      }
      break;
    case CODES_WK_IRECV:
    case CODES_WK_RECV:
      {
        codes_exec_mpi_recv_rc(s, bf, m, lp);
        s->num_recvs--;
        s->ross_sample.num_recvs--;
      }
      break;
    case CODES_WK_DELAY:
      {
        s->num_delays--;
        if (disable_compute)
          issue_next_event_rc(lp);
        else
          codes_exec_comp_delay_rc(s, bf, m, lp);
      }
      break;
    case CODES_WK_ALLREDUCE:
      {
        if (bf->c27) {
          s->num_all_reduce--;
          s->col_time = m->rc.saved_send_time;
          s->all_reduce_time = m->rc.saved_delay;
        }
        else {
          s->col_time = 0;
        }
        issue_next_event_rc(lp);
      }
      break;
    case CODES_WK_BCAST:
    case CODES_WK_ALLGATHER:
    case CODES_WK_ALLGATHERV:
    case CODES_WK_ALLTOALL:
    case CODES_WK_ALLTOALLV:
    case CODES_WK_REDUCE:
    case CODES_WK_COL:
      {
        s->num_cols--;
        issue_next_event_rc(lp);
      }
      break;
    case CODES_WK_WAITSOME:
    case CODES_WK_WAITANY:
      {
        s->num_waitsome--;
        issue_next_event_rc(lp);
      }
      break;
    case CODES_WK_WAIT:
      {
        s->num_wait--;
        codes_exec_mpi_wait_rc(s, bf, lp, m);
      }
      break;
    case CODES_WK_WAITALL:
      {
        s->num_waitall--;
        codes_exec_mpi_waitall_rc(s, bf, m, lp);
      }
      break;
    default:
      printf("Invalid op type %d\n", m->op_type);
  }
}

void nw_test_finalize(nw_state* s, tw_lp* lp)
{
  // Check if there are incomplete waits
  if (s->wait_op) {
    printf("\nIncomplete wait operation on rank %llu\n", s->nw_id);
    print_waiting_reqs(s->wait_op);
    print_completed_queue(lp, &s->completed_reqs);
  }

  // Early termination if LP is invalid
  if (alloc_spec == 1) {
    struct codes_jobmap_id lid;
    lid = codes_jobmap_to_local_id(s->nw_id, jobmap_ctx);

    if (lid.job < 0)
      return;
  }
  else {
    if (s->nw_id >= (tw_lpid)num_net_traces)
      return;
  }

  // Print message tracking info
  if (enable_msg_tracking) {
    if (s->local_rank == 0)
      fprintf(msg_log_file, "rank msg size num_msgs agg_latency avg_latency\n");

    struct msg_size_info* tmp_msg = NULL;
    struct qlist_head* ent = NULL;
    qlist_for_each(ent, &s->msg_sz_list) {
      tmp_msg = qlist_entry(ent, struct msg_size_info, ql);
      printf("rank %d msg size %"PRId64" num_msgs %d agg_latency %f avg_latency %f\n",
          s->local_rank, tmp_msg->msg_size, tmp_msg->num_msgs, tmp_msg->agg_latency, tmp_msg->avg_latency);

      if (s->local_rank == 0) {
        fprintf(msg_log_file, "%llu %"PRId64" %d %f\n",
            LLU(s->nw_id), tmp_msg->msg_size, tmp_msg->num_msgs, tmp_msg->avg_latency);
      }
    }
  }

  // Check for unmatched MPI operations
  int count_irecv = qlist_count(&s->pending_recvs_queue);
  int count_isend = qlist_count(&s->arrival_queue);
  if (count_irecv > 0 || count_isend > 0) {
    unmatched = 1;
    printf("nw-id %lld unmatched irecvs %d unmatched isends %d"
        "total sends %ld receives %ld collectives %ld delays %ld"
        "wait alls %ld waits %ld send time %lf wait %lf",
        s->nw_id, count_irecv, count_isend, s->num_sends, s->num_recvs, s->num_cols,
        s->num_delays, s->num_waitall, s->num_wait, s->send_time, s->wait_time);
    print_msgs_queue(&s->pending_recvs_queue, 0);
    print_msgs_queue(&s->arrival_queue, 1);
  }

  // Write replay stats
  int written = 0;
  if (!s->nw_id)
    written = sprintf(s->output_buf, "# Format <LP ID> <Terminal ID> <Job ID> "
        "<Local Rank> <Total sends> <Total Recvs> <Bytes sent> <Bytes recvd> "
        "<Send time> <Comm. time> <Compute time> <Max Msg Time>\n");
  written += sprintf(s->output_buf + written, "%llu %llu %d %d %ld %ld %ld %ld %lf %lf %lf %lf\n",
      LLU(lp->gid), LLU(s->nw_id), s->app_id, s->local_rank, s->num_sends, s->num_recvs, s->num_bytes_sent,
      s->num_bytes_recvd, s->send_time, s->elapsed_time - s->compute_time, s->compute_time, s->max_time);
  lp_io_write(lp->gid, (char*)"mpi-replay-stats", written, s->output_buf);

  // Update maximum times
  if (s->elapsed_time - s->compute_time > max_comm_time)
    max_comm_time = s->elapsed_time - s->compute_time;
  if (s->elapsed_time > max_time)
    max_time = s->elapsed_time;
  if(s->wait_time > max_wait_time)
    max_wait_time = s->wait_time;
  if(s->send_time > max_send_time)
    max_send_time = s->send_time;
  if(s->recv_time > max_recv_time)
    max_recv_time = s->recv_time;

  // Update average times
  avg_time += s->elapsed_time;
  avg_comm_time += (s->elapsed_time - s->compute_time);
  avg_wait_time += s->wait_time;
  avg_send_time += s->send_time;
  avg_recv_time += s->recv_time;

  if (enable_sampling) {
    fseek(agg_log_file, sample_bytes_written, SEEK_SET);
    fwrite(s->mpi_wkld_samples, sizeof(struct mpi_workload_sample), s->sampling_indx + 1, agg_log_file);
    sample_bytes_written += (s->sampling_indx * sizeof(struct mpi_workload_sample));
  }

  if (debug_cols) {
    written = sprintf(s->col_stats, "%llu\t%lf s\n", LLU(s->nw_id), ns_to_s(s->all_reduce_time / s->num_all_reduce));
    lp_io_write(lp->gid, (char*)"avg-all-reduce-time", written, s->col_stats);
  }

  // Destroy RC stacks
  rc_stack_destroy(s->rc_matched_reqs);
  rc_stack_destroy(s->rc_processed_ops);
  rc_stack_destroy(s->rc_processed_wait_op);
}

const tw_optdef app_opt [] =
{
  TWOPT_GROUP("Network workload test"),
  TWOPT_CHAR("workload_type", workload_type, "dumpi"),
  TWOPT_CHAR("workload_file", workload_file, "workload file name"),
  TWOPT_CHAR("alloc_file", alloc_file, "allocation file name"),
  TWOPT_UINT("num_net_traces", num_net_traces, "number of network traces"),
  TWOPT_UINT("priority_type", priority_type, "priority type (zero): high priority to foreground traffic and low to background/2nd job, (one): high priority to collective operations"),
  TWOPT_UINT("disable_compute", disable_compute, "disable compute simulation"),
  TWOPT_UINT("debug_cols", debug_cols, "completion time of collective operations (currently MPI_AllReduce)"),
  TWOPT_UINT("enable_mpi_debug", enable_debug, "enable debugging of MPI sim layer (works with sync=1 only)"),
  TWOPT_UINT("sampling_interval", sampling_interval, "sampling interval for MPI operations"),
  TWOPT_UINT("enable_sampling", enable_sampling, "enable sampling (only works in sequential mode)"),
  TWOPT_STIME("sampling_end_time", sampling_end_time, "sampling_end_time"),
  TWOPT_CHAR("lp-io-dir", lp_io_dir, "where to place LP-IO output (unspecified -> no output)"),
  TWOPT_UINT("lp-io-use-suffix", lp_io_use_suffix, "whether to append unique suffix to LP-IO directory (default 0)"),
#ifdef ENABLE_CORTEX_PYTHON
  TWOPT_CHAR("cortex-file", cortex_file, "Python file (without .py) containing the CoRtEx translation class"),
  TWOPT_CHAR("cortex-class", cortex_class, "Python class implementing the CoRtEx translator"),
  TWOPT_CHAR("cortex-gen", cortex_gen, "Python function to pre-generate MPI events"),
#endif
  TWOPT_END()
};

tw_lptype nw_lp = {
  (init_f) nw_test_init,
  (pre_run_f) NULL,
  (event_f) nw_test_event_handler,
  (revent_f) nw_test_event_handler_rc,
  (commit_f) NULL,
  (final_f) nw_test_finalize,
  (map_f) codes_mapping,
  sizeof(nw_state)
};

const tw_lptype* nw_get_lp_type()
{
  return (&nw_lp);
}

static void nw_add_lp_type()
{
  lp_type_register("nw-lp", nw_get_lp_type());
}

/* Setup for ROSS event tracing */
void nw_lp_event_collect(nw_message* m, tw_lp* lp, char* buffer, int* collect_flag)
{
  (void)lp;
  (void)collect_flag;

  int type = m->msg_type;
  memcpy(buffer, &type, sizeof(type));
}

// Any model level data can be added for collection along with simulation data
// inside ROSS. Last field in nw_lp_model_types[0] will need to be updated
// for the size of the data to save in each function call.
void nw_lp_model_stat_collect(nw_state* s, tw_lp* lp, char* buffer)
{
  (void)s;
  (void)lp;
  (void)buffer;
}

void ross_nw_lp_sample_fn(nw_state* s, tw_bf* bf, tw_lp* lp, struct ross_model_sample* sample)
{
  memcpy(sample, &s->ross_sample, sizeof(s->ross_sample));
  sample->nw_id = s->nw_id;
  sample->app_id = s->app_id;
  sample->local_rank = s->local_rank;
  sample->comm_time = s->elapsed_time - s->compute_time;
  if (alloc_spec == 1) {
    struct codes_jobmap_id lid;
    lid = codes_jobmap_to_local_id(s->nw_id, jobmap_ctx);
  }
  memset(&s->ross_sample, 0, sizeof(s->ross_sample));
}

void ross_nw_lp_sample_rc_fn(nw_state* s, tw_bf* bf, tw_lp* lp, struct ross_model_sample* sample)
{
  memcpy(&s->ross_sample, sample, sizeof(*sample));
}

st_model_types nw_lp_model_types[] = {
  {(ev_trace_f) nw_lp_event_collect,
    sizeof(int),
    (model_stat_f) nw_lp_model_stat_collect,
    0,
    (sample_event_f) ross_nw_lp_sample_fn,
    (sample_revent_f) ross_nw_lp_sample_rc_fn,
    sizeof(struct ross_model_sample)},
  {NULL, 0, NULL, 0, NULL, NULL, 0}
};

static const st_model_types* nw_lp_get_model_stat_types(void)
{
  return (&nw_lp_model_types[0]);
}

void nw_lp_register_model()
{
  st_model_type_register("nw-lp", nw_lp_get_model_stat_types());
}
/* End of ROSS event tracing setup */

// Function for comparing two hash values
static int msg_size_hash_compare(void* key, struct qhash_head* link)
{
  int64_t* in_size = (int64_t*)key;
  struct msg_size_info* tmp;

  tmp = qhash_entry(link, struct msg_size_info, hash_link);
  if (tmp->msg_size == *in_size)
    return 1;

  return 0;
}

// Read configuration file
void modelnet_mpi_replay_read_config()
{
  // Load the factor by which the compute time is sped up by.
  // e.g. If compute_time_speedup = 2, all compute time delay is halved.
  configuration_get_value_double(&config, "PARAMS", "compute_time_speedup", NULL, &compute_time_speedup);
  configuration_get_value_double(&config, "PARAMS", "self_msg_overhead", NULL, &self_overhead);
  configuration_get_value_double(&config, "PARAMS", "nic_delay", NULL, &nic_delay);
  configuration_get_value_double(&config, "PARAMS", "soft_delay", NULL, &soft_delay_mpi);
  configuration_get_value_double(&config, "PARAMS", "copy_per_byte", NULL, &copy_per_byte_eager);
  configuration_get_value_int(&config, "PARAMS", "eager_limit", NULL, (int*)&eager_limit);
  printf("compute_time_speedup: %lf, self_msg_overhead: %lf, nic_delay: %lf, "
      "soft_delay: %lf, copy_per_byte: %lf, eager_limit: %llu\n", compute_time_speedup,
      self_overhead, nic_delay, soft_delay_mpi, copy_per_byte_eager, eager_limit);
}

// Main
int modelnet_mpi_replay(MPI_Comm comm, int* argc, char*** argv )
{
  // Set up MPI communicator
  int rank;
  MPI_COMM_CODES = comm;
  MPI_Comm_rank(MPI_COMM_CODES, &rank);
  MPI_Comm_size(MPI_COMM_CODES, &nprocs);

  tw_comm_set(MPI_COMM_CODES);

  g_tw_ts_end = s_to_ns(60*60); // One hour in ns

  // Add options and initialize ROSS
  workload_type[0]='\0';
  tw_opt_add(app_opt);
  tw_init(argc, argv);

  codes_comm_update();

  // Currently only DUMPI is supported as the workload
  if (strcmp(workload_type, "dumpi") != 0) {
	  if (tw_ismaster()) {
      printf("Usage: mpirun -np n ./modelnet-mpi-replay --sync=1/3"
             " --workload_type=dumpi"
             " --alloc_file=alloc-file-name"
#ifdef ENABLE_CORTEX_PYTHON
             " --cortex-file=cortex-file-name"
             " --cortex-class=cortex-class-name"
             " --cortex-gen=cortex-function-name"
#endif
             " -- network-config-file\n"
             "See model-net/doc/README.dragonfly.txt and model-net/doc/README.torus.txt"
             " for instructions on how to run the models with network traces\n");
    }
    tw_end();
    return -1;
  }

  // Set up workload traces
  num_traces_of_job[0] = num_net_traces;
  if (strcmp(workload_type, "dumpi") == 0) {
    assert(strlen(workload_file) > 0);
    strcpy(file_name_of_job[0], workload_file);
  }
  alloc_spec = 0;
  if (strlen(alloc_file) > 0) {
    alloc_spec = 1;
    jobmap_p.alloc_file = alloc_file;
    jobmap_ctx = codes_jobmap_configure(CODES_JOBMAP_LIST, &jobmap_p);
  }

  // Number of traces should be larger than 0 at this point
  assert(num_net_traces);

  // Load configuration parameters into CODES
  configuration_load((*argv)[2], MPI_COMM_CODES, &config);

  // Register LPs
  nw_add_lp_type();
  model_net_register();

  if (g_st_ev_trace || g_st_model_stats || g_st_use_analysis_lps)
    nw_lp_register_model();

  int num_nets;
  int* net_ids = model_net_configure(&num_nets);
  assert(num_nets == 1);
  net_id = net_ids[0];
  free(net_ids);

  // Read MPI-replay parameters from config file
  modelnet_mpi_replay_read_config();

  if (enable_debug) {
    debug_log_file = fopen("mpi-op-logs", "w+");
    if (!debug_log_file) {
      printf("Error opening workload log file\n");
      MPI_Abort(MPI_COMM_CODES, MPI_ERR_OTHER);
    }
  }

  // Get current time to use in output file names
  time_t cur_time;
  struct tm* time_info;
  time(&cur_time);
  time_info = localtime(&cur_time);
  char cur_time_str[32];
  sprintf(cur_time_str, "%d%02d%02d-%02d%02d%02d", 1900 + time_info->tm_year,
      1 + time_info->tm_mon, time_info->tm_mday, time_info->tm_hour,
      time_info->tm_min, time_info->tm_sec);

  // Create message info file
  if (enable_msg_tracking) {
    char mpi_msg_dir[32];
    sprintf(mpi_msg_dir, "mpi_msg_dir");
    mkdir(mpi_msg_dir, S_IRUSR | S_IWUSR | S_IXUSR);

    char msg_log_name[128];
    sprintf(msg_log_name, "%s/log-%s", mpi_msg_dir, cur_time_str);

    msg_log_file = fopen(msg_log_name, "w+");
    if (!msg_log_file) {
      printf("Error creating MPI message log file\n");
      MPI_Abort(MPI_COMM_CODES, MPI_ERR_OTHER);
    }
  }

  // Set up sampling
  if (enable_sampling) {
    char sampling_dir[32];
    sprintf(sampling_dir, "sampling_dir");
    mkdir(sampling_dir, S_IRUSR | S_IWUSR | S_IXUSR);

    char sampling_agg_log_name[128];
    char sampling_meta_log_name[128];
    sprintf(sampling_agg_log_name, "%s/agg-%s-%d.bin", sampling_dir, cur_time_str, rank);
    sprintf(sampling_meta_log_name, "%s/meta-%s", sampling_dir, cur_time_str);
    agg_log_file = fopen(sampling_agg_log_name, "w+");
    meta_log_file = fopen(sampling_meta_log_name, "w+");
    if (!agg_log_file || !meta_log_file) {
      printf("Error creating MPI sampling log files\n");
      MPI_Abort(MPI_COMM_CODES, MPI_ERR_OTHER);
    }

    model_net_enable_sampling(sampling_interval, sampling_end_time);
  }

  // CODES mapping context
  // See codes/codes-mapping-context.h for more information.
  // THe current default is GROUP_MODULO.
  switch (mctx_type) {
    case GROUP_RATIO:
      mapping_context = codes_mctx_set_group_ratio(NULL, true);
      break;
    case GROUP_RATIO_REVERSE:
      mapping_context = codes_mctx_set_group_ratio_reverse(NULL, true);
      break;
    case GROUP_DIRECT:
      mapping_context = codes_mctx_set_group_direct(1,NULL, true);
      break;
    case GROUP_MODULO:
      mapping_context = codes_mctx_set_group_modulo(NULL, true);
      break;
    case GROUP_MODULO_REVERSE:
      mapping_context = codes_mctx_set_group_modulo_reverse(NULL, true);
      break;
  }

  codes_mapping_setup();

  total_ranks = codes_mapping_get_lp_count("MODELNET_GRP", 0, "nw-lp", NULL, 0);
  ranks_per_node = codes_mapping_get_lp_count("MODELNET_GRP", 1, "nw-lp", NULL, 1); // Ignores repetitions & annotations

  // Prepare LP-IO
  if (lp_io_dir[0]) {
    do_lp_io = 1;
    int flags = lp_io_use_suffix ? LP_IO_UNIQ_SUFFIX : 0;
    int ret = lp_io_prepare(lp_io_dir, flags, &io_handle, MPI_COMM_CODES);
    if (ret) {
      printf("Error in preparing LP-IO\n");
      MPI_Abort(MPI_COMM_CODES, MPI_ERR_OTHER);
    }
  }

  // Start the simulation
  tw_run();

  // Simulation done, close log files
  if (enable_debug)
    fclose(debug_log_file);

  if (enable_msg_tracking)
    fclose(msg_log_file);

  if (enable_sampling) {
    fclose(agg_log_file);
    fclose(meta_log_file);
  }

  // Gather statistics from all ranks
  long long total_bytes_sent, total_bytes_recvd;
  double max_run_time, avg_run_time;
  double max_comm_run_time, avg_comm_run_time;
  double total_avg_send_time, total_max_send_time;
  double total_avg_wait_time, total_max_wait_time;
  double total_avg_recv_time, total_max_recv_time;

  MPI_Reduce(&num_bytes_sent, &total_bytes_sent, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_CODES);
  MPI_Reduce(&num_bytes_recvd, &total_bytes_recvd, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_CODES);
  MPI_Reduce(&max_comm_time, &max_comm_run_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_CODES);
  MPI_Reduce(&max_time, &max_run_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_CODES);
  MPI_Reduce(&avg_time, &avg_run_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_CODES);

  MPI_Reduce(&avg_recv_time, &total_avg_recv_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_CODES);
  MPI_Reduce(&avg_comm_time, &avg_comm_run_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_CODES);
  MPI_Reduce(&max_wait_time, &total_max_wait_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_CODES);
  MPI_Reduce(&max_send_time, &total_max_send_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_CODES);
  MPI_Reduce(&max_recv_time, &total_max_recv_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_CODES);
  MPI_Reduce(&avg_wait_time, &total_avg_wait_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_CODES);
  MPI_Reduce(&avg_send_time, &total_avg_send_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_CODES);

  if (!g_tw_mynode) {
    printf("\nTotal bytes sent %lld recvd %lld\n"
        "max runtime %lf ns, avg runtime %lf ns\n"
        "max comm time %lf ns, avg comm time %lf ns\n"
        "max send time %lf ns, avg send time %lf ns\n"
        "max recv time %lf ns, avg recv time %lf ns\n"
        "max wait time %lf ns, avg wait time %lf ns\n",
        total_bytes_sent, total_bytes_recvd,
        max_run_time, avg_run_time / num_net_traces,
        max_comm_run_time, avg_comm_run_time / num_net_traces,
        total_max_send_time, total_avg_send_time / num_net_traces,
        total_max_recv_time, total_avg_recv_time / num_net_traces,
        total_max_wait_time, total_avg_wait_time / num_net_traces);
  }

  // Flush LP-IO
  if (do_lp_io) {
    int ret = lp_io_flush(io_handle, MPI_COMM_CODES);
    if (ret) {
      printf("Error in LP-IO flush\n");
      MPI_Abort(MPI_COMM_CODES, MPI_ERR_OTHER);
    }
  }

  // Print model-net stats
  model_net_report_stats(net_id);

  if (unmatched && g_tw_mynode == 0)
    fprintf(stderr, "\nWarning: unmatched send and receive operations found\n");

  if (alloc_spec && jobmap_ctx)
    codes_jobmap_destroy(jobmap_ctx);

  // Terminate ROSS
  tw_end();

  return 0;
}
