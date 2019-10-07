/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <string.h>
#include <assert.h>
#include <ross.h>

#include "codes/lp-io.h"
#include "codes/jenkins-hash.h"
#include "codes/model-net-method.h"
#include "codes/model-net.h"
#include "codes/model-net-lp.h"
#include "codes/codes_mapping.h"
#include "codes/codes.h"
#include "codes/net/simplenet-upd.h"

#define CATEGORY_NAME_MAX 16
#define CATEGORY_MAX 12

#define N_PARAM_TYPES 3

#define LP_CONFIG_NM (model_net_lp_config_names[SIMPLENET])
#define LP_METHOD_NM (model_net_method_names[SIMPLENET])

/* structs for initializing a network/ specifying network parameters */
struct simplenet_param
{
    int k_value;
    int short_limit;
    int eager_limit;
    double short_a;
    double short_b;
    double eager_a;
    double eager_rcb;
    double eager_rci;
    double rend_a;
    double rend_rcb;
    double rend_rci;
};
typedef struct simplenet_param simplenet_param;

/*Define simplenet data types and structs*/
typedef struct sn_state sn_state;

struct sn_state
{
    /* next idle times for network card, both inbound and outbound */
    tw_stime net_send_next_idle;
    tw_stime net_recv_next_idle;
    const char * anno;
    const simplenet_param* params[N_PARAM_TYPES]; // intra-socket, inter-socket, inter-node
    struct mn_stats sn_stats_array[CATEGORY_MAX];
};

/* annotation-specific parameters (unannotated entry occurs at the
 * last index) */
static uint64_t                  num_params = 0;
static simplenet_param         * all_params = NULL;
static const config_anno_map_t * anno_map   = NULL;

static int node_size = 1;
static int socket_size = 1;

static int sn_magic = 0;

/* returns a pointer to the lptype struct to use for simplenet LPs */
static const tw_lptype* sn_get_lp_type(void);

/* retrieve the size of the portion of the event struct that is consumed by
 * the simplenet module.  The caller should add this value to the size of
 * its own event structure to get the maximum total size of a message.
 */
static int sn_get_msg_sz(void);

/* Returns the simplenet magic number */
static int sn_get_magic();

#if SIMPLENET_DEBUG
static void print_msg(sn_message *m);
#endif

/* collective network calls */
static void simple_net_collective();

/* collective network calls-- rc */
static void simple_net_collective_rc();

/* Modelnet interface events */
/* sets up the simplenet parameters through modelnet interface */
static void sn_configure();

void nodes_set_params(const char* config_file, simplenet_param* params);

/* allocate a new event that will pass through simplenet to arriave at its
 * destination:
 *
 * - category: category name to associate with this communication
 * - final_dest_gid: the LP that the message should be delivered to.
 * - event_size_bytes: size of event msg that will be delivered to
 * final_dest_gid.
 * - local_event_size_byte: size of event message that will delivered to
 *   local LP upon local send comletion (set to 0 if not used)
 * - net_msg_size_bytes: size of simulated network message in bytes.
 * - sender: LP calling this function.
 */
/* Issues a simplenet packet event call */
static tw_stime simplenet_packet_event(
        model_net_request const * req,
        uint64_t message_offset,
        uint64_t packet_size,
        tw_stime offset,
        mn_sched_params const * sched_params,
        void const * remote_event,
        void const * self_event,
        tw_lp *sender,
        int is_last_pckt);

static void simplenet_packet_event_rc(tw_lp *sender);

static void simplenet_packet_event_rc(tw_lp *sender);

static void sn_report_stats();

/* data structure for model-net statistics */
struct model_net_method simplenet_method =
{
    .mn_configure = sn_configure,
    .mn_register = NULL,
    .model_net_method_packet_event = simplenet_packet_event,
    .model_net_method_packet_event_rc = simplenet_packet_event_rc,
    .model_net_method_recv_msg_event = NULL,
    .model_net_method_recv_msg_event_rc = NULL,
    .mn_get_lp_type = sn_get_lp_type,
    .mn_get_msg_sz = sn_get_msg_sz,
    .mn_report_stats = sn_report_stats,
    .mn_collective_call = simple_net_collective,
    .mn_collective_call_rc = simple_net_collective_rc,
    .mn_sample_fn = NULL,
    .mn_sample_rc_fn = NULL,
    .mn_sample_init_fn = NULL,
    .mn_sample_fini_fn = NULL,
    .mn_model_stat_register = NULL, // for ROSS instrumentation
    .mn_get_model_stat_types = NULL // for ROSS instrumentation
};

static void sn_init(
    sn_state * ns,
    tw_lp * lp);
static void sn_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp);
static void sn_rev_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp);
static void sn_finalize(
    sn_state * ns,
    tw_lp * lp);

tw_lptype sn_lp = {
    (init_f) sn_init,
    (pre_run_f) NULL,
    (event_f) sn_event,
    (revent_f) sn_rev_event,
    (commit_f) NULL,
    (final_f) sn_finalize,
    (map_f) codes_mapping,
    sizeof(sn_state),
};

static tw_stime rate_to_ns(uint64_t bytes, double MB_p_s);
static void handle_msg_ready_rev_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp);
static void handle_msg_ready_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp);
static void handle_msg_start_rev_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp);
static void handle_msg_start_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp);

/* returns pointer to LP information for simplenet module */
static const tw_lptype* sn_get_lp_type()
{
    return(&sn_lp);
}

/* returns number of bytes that the simplenet module will consume in event
 * messages
 */
static int sn_get_msg_sz(void)
{
    return(sizeof(sn_message));
}
/* collective network calls */
static void simple_net_collective()
{
/* collectives not supported */
    return;
}

/* collective network call -- rc*/
static void simple_net_collective_rc()
{
/* collectives not supported */
   return;
}

/* report network statistics */
static void sn_report_stats()
{
   /* TODO: Do we have some simplenet statistics to report like we have for torus and dragonfly? */
   return;
}
static void sn_init(
    sn_state * ns,
    tw_lp * lp)
{
    uint32_t h1 = 0, h2 = 0;
    memset(ns, 0, sizeof(*ns));

    /* all devices are idle to begin with */
    ns->net_send_next_idle = tw_now(lp);
    ns->net_recv_next_idle = tw_now(lp);

    int i = 0;
    ns->anno = codes_mapping_get_annotation_by_lpid(lp->gid);
    if (ns->anno == NULL) {
        for (i = 0; i < N_PARAM_TYPES; i++) {
            ns->params[i] = &all_params[num_params-N_PARAM_TYPES+i];
        }
    }
    else {
        int id = configuration_get_annotation_index(ns->anno, anno_map);
        for (i = 0; i < N_PARAM_TYPES; i++) {
            ns->params[i] = &all_params[id+i];
        }
    }

    bj_hashlittle2(LP_METHOD_NM, strlen(LP_METHOD_NM), &h1, &h2);
    sn_magic = h1+h2;
    /* printf("\n sn_magic %d ", sn_magic); */

    return;
}

static void sn_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp)
{
    (void)b; // bitflags aren't used in simplenet
    assert(m->magic == sn_magic);

    switch (m->event_type)
    {
        case SN_MSG_START:
            handle_msg_start_event(ns, m, lp);
            break;
        case SN_MSG_READY:
            handle_msg_ready_event(ns, m, lp);
            break;
        default:
            assert(0);
            break;
    }
}

static void sn_rev_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp)
{
    (void)b;
    assert(m->magic == sn_magic);

    switch (m->event_type)
    {
        case SN_MSG_START:
            handle_msg_start_rev_event(ns, m, lp);
            break;
        case SN_MSG_READY:
            handle_msg_ready_rev_event(ns, m, lp);
            break;
        default:
            assert(0);
            break;
    }

    return;
}

static void sn_finalize(
    sn_state * ns,
    tw_lp * lp)
{
    model_net_print_stats(lp->gid, &ns->sn_stats_array[0]);
    return;
}

int sn_get_magic()
{
  return sn_magic;
}

/* convert bytes and us/B to ns */
static tw_stime rate_to_ns(uint64_t bytes, double per_byte_cost)
{
    tw_stime time;

    time = (double)bytes * per_byte_cost; // us
    time *= 1000; // convert to ns

    return(time);
}

static int get_param_index(tw_lpid src, tw_lpid dest) {
    int param_index = 2;

    int src_rel_id = codes_mapping_get_lp_relative_id(src, 0, 0);
    int dest_rel_id = codes_mapping_get_lp_relative_id(dest, 0, 0);

    int src_node_id = src_rel_id / node_size;
    int dest_node_id = dest_rel_id / node_size;

    if (src_node_id != dest_node_id) {
        // Inter-node
        param_index = 2;
    }
    else {
        int src_local_id = src_rel_id % node_size;
        int dest_local_id = dest_rel_id % node_size;
        int src_socket_id = src_local_id / socket_size;
        int dest_socket_id = dest_local_id / socket_size;

        if (src_socket_id != dest_socket_id) {
            // Inter-socket
            param_index = 1;
        }
        else {
            // Intra-socket
            param_index = 0;
        }
    }

    return param_index;
}

/* reverse computation for msg ready event */
static void handle_msg_ready_rev_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp)
{
    struct mn_stats* stat;

    ns->net_recv_next_idle = m->net_recv_next_idle_saved;

    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->recv_count--;
    stat->recv_bytes -= m->net_msg_size_bytes;
    stat->recv_time = m->recv_time_saved;

    if (m->event_size_bytes && m->is_pull){
        model_net_event_rc2(lp, &m->event_rc);
    }

    return;
}

/* handler for msg ready event.  This indicates that a message is available
 * to recv, but we haven't checked to see if the recv queue is available yet
 */
static void handle_msg_ready_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp)
{
    tw_stime recv_queue_time = 0;
    struct mn_stats* stat;

    // Determine model parameters
    int param_index = get_param_index(m->src_mn_lp, lp->gid);
    const simplenet_param* param = ns->params[param_index];
    double per_byte_cost; // us/B

    if (m->net_msg_size_bytes <= param->short_limit) {
        // Short
        per_byte_cost = (double)param->k_value * param->short_b;
    }
    else if (m->net_msg_size_bytes <= param->eager_limit) {
        // Eager
        per_byte_cost = (double)param->k_value / (param->eager_rcb + (double)(param->k_value - 1) * param->eager_rci);
    }
    else {
        // Rendezvous
        per_byte_cost = (double)param->k_value / (param->rend_rcb + (double)(param->k_value - 1) * param->rend_rci);
    }

    //printf("handle_msg_ready_event(), lp %llu.\n", (unsigned long long)lp->gid);
    /* add statistics */
    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->recv_count++;
    stat->recv_bytes += m->net_msg_size_bytes;
    m->recv_time_saved = stat->recv_time;
    stat->recv_time += rate_to_ns(m->net_msg_size_bytes, per_byte_cost);

    /* are we available to recv the msg? */
    /* were we available when the transmission was started? */
    if(ns->net_recv_next_idle > tw_now(lp))
        recv_queue_time += ns->net_recv_next_idle - tw_now(lp);

    /* calculate transfer time based on msg size and bandwidth */
    recv_queue_time += rate_to_ns(m->net_msg_size_bytes, per_byte_cost);

    /* bump up input queue idle time accordingly */
    m->net_recv_next_idle_saved = ns->net_recv_next_idle;
    ns->net_recv_next_idle = recv_queue_time + tw_now(lp);

    /* copy only the part of the message used by higher level */
    if(m->event_size_bytes)
    {
        //char *tmp_ptr = (char*)m;
        //tmp_ptr += sn_get_msg_sz();
        void *tmp_ptr = model_net_method_get_edata(SIMPLENET, m);
      /* schedule event to final destination for when the recv is complete */
//      printf("\n Remote message to LP %d ", m->final_dest_gid);
        if (m->is_pull){
            /* call the model-net event, using direct contexts for mapping (we
             * know all involved LPs */
            struct codes_mctx mc_dst =
                codes_mctx_set_global_direct(m->src_mn_lp);
            struct codes_mctx mc_src =
                codes_mctx_set_global_direct(lp->gid);
            int net_id = model_net_get_id(LP_METHOD_NM);
            m->event_rc = model_net_event_mctx(net_id, &mc_src, &mc_dst,
                    m->category, m->src_gid, m->pull_size, recv_queue_time,
                    m->event_size_bytes, tmp_ptr, 0, NULL, lp);
        }
        else{
            tw_event * e_new = tw_event_new(m->final_dest_gid, recv_queue_time, lp);
            void * m_new = tw_event_data(e_new);
            memcpy(m_new, tmp_ptr, m->event_size_bytes);
            tw_event_send(e_new);
        }
    }

    return;
}

/* reverse computation for msg start event */
static void handle_msg_start_rev_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp)
{
    ns->net_send_next_idle = m->net_send_next_idle_saved;

    codes_local_latency_reverse(lp);

    if(m->local_event_size_bytes > 0)
    {
        codes_local_latency_reverse(lp);
    }

    mn_stats* stat;
    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->send_count--;
    stat->send_bytes -= m->net_msg_size_bytes;
    stat->send_time = m->send_time_saved;

    return;
}

/* handler for msg start event; this indicates that the caller is trying to
 * transmit a message through this NIC
 */
static void handle_msg_start_event(
    sn_state * ns,
    sn_message * m,
    tw_lp * lp)
{
    tw_event *e_new;
    sn_message *m_new;
    tw_stime send_queue_time = 0;
    mn_stats* stat;
    int total_event_size;

    total_event_size = model_net_get_msg_sz(SIMPLENET) + m->event_size_bytes +
        m->local_event_size_bytes;

    // Determine model parameters
    int param_index = get_param_index(lp->gid, m->dest_mn_lp);
    const simplenet_param* param = ns->params[param_index];
    double start_up = 0;
    double per_byte_cost = 0;

    if (m->net_msg_size_bytes <= param->short_limit) {
        // Short
        start_up = param->short_a;
        per_byte_cost = (double)param->k_value * param->short_b;
    }
    else if (m->net_msg_size_bytes <= param->eager_limit) {
        // Eager
        start_up = param->eager_a;
        per_byte_cost = (double)param->k_value / (param->eager_rcb + (double)(param->k_value - 1) * param->eager_rci);
    }
    else {
        // Rendezvous
        start_up = param->rend_a;
        per_byte_cost = (double)param->k_value / (param->rend_rcb + (double)(param->k_value - 1) * param->rend_rci);
    }
    start_up *= 1000; // convert from us to ns

    //printf("handle_msg_start_event(), lp %llu.\n", (unsigned long long)lp->gid);
    /* add statistics */
    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->send_count++;
    stat->send_bytes += m->net_msg_size_bytes;
    m->send_time_saved = stat->send_time;
    stat->send_time += (start_up + rate_to_ns(m->net_msg_size_bytes, per_byte_cost));
    if(stat->max_event_size < total_event_size)
        stat->max_event_size = total_event_size;

    /* calculate send time stamp */
    send_queue_time = start_up; /* net msg startup cost */
    /* bump up time if the NIC send queue isn't idle right now */
    if(ns->net_send_next_idle > tw_now(lp))
        send_queue_time += ns->net_send_next_idle - tw_now(lp);

    /* move the next idle time ahead to after this transmission is
     * _complete_ from the sender's perspective
     */
    m->net_send_next_idle_saved = ns->net_send_next_idle;
    ns->net_send_next_idle = send_queue_time + tw_now(lp) +
        rate_to_ns(m->net_msg_size_bytes, per_byte_cost);

    void *m_data;
    e_new = model_net_method_event_new(m->dest_mn_lp, send_queue_time, lp,
            SIMPLENET, (void**)&m_new, &m_data);

    /* copy entire previous message over, including payload from user of
     * this module
     */
    //memcpy(m_new, m, m->event_size_bytes + model_net_get_msg_sz(SIMPLENET));
    memcpy(m_new, m, sizeof(sn_message));
    if (m->event_size_bytes){
        memcpy(m_data, model_net_method_get_edata(SIMPLENET, m),
                m->event_size_bytes);
    }

    m_new->event_type = SN_MSG_READY;

    //print_base_from(SIMPLENET, m_new);
    //print_msg(m_new);
    tw_event_send(e_new);

    // now that message is sent, issue an "idle" event to tell the scheduler
    // when I'm next available
    model_net_method_idle_event(codes_local_latency(lp) +
            ns->net_send_next_idle - tw_now(lp), 0, lp);

    /* if there is a local event to handle, then create an event for it as
     * well
     */
    if(m->local_event_size_bytes > 0)
    {
        //char* local_event;

        e_new = tw_event_new(m->src_gid, send_queue_time+codes_local_latency(lp), lp);
        m_new = tw_event_data(e_new);

        void * m_loc = (char*) model_net_method_get_edata(SIMPLENET, m) +
            m->event_size_bytes;

         //local_event = (char*)m;
         //local_event += model_net_get_msg_sz(SIMPLENET) + m->event_size_bytes;
        /* copy just the local event data over (which is past the remote event
         * in memory) */
        memcpy(m_new, m_loc, m->local_event_size_bytes);
        tw_event_send(e_new);
    }
    return;
}

/* Model-net function calls */

/*This method will serve as an intermediate layer between simplenet and modelnet.
 * It takes the packets from modelnet layer and calls underlying simplenet methods*/
static tw_stime simplenet_packet_event(
        model_net_request const * req,
        uint64_t message_offset,
        uint64_t packet_size,
        tw_stime offset,
        mn_sched_params const * sched_params,
        void const * remote_event,
        void const * self_event,
        tw_lp *sender,
        int is_last_pckt)
{
     (void)message_offset; // unused...
     (void)sched_params; // unused...

     tw_event * e_new;
     tw_stime xfer_to_nic_time;
     sn_message * msg;
     char* tmp_ptr;

     xfer_to_nic_time = codes_local_latency(sender);
     // this is a self message
     e_new = model_net_method_event_new(sender->gid, xfer_to_nic_time+offset,
             sender, SIMPLENET, (void**)&msg, (void**)&tmp_ptr);
     strcpy(msg->category, req->category);
     msg->src_gid = req->src_lp;
     msg->src_mn_lp = sender->gid;
     msg->final_dest_gid = req->final_dest_lp;
     msg->dest_mn_lp = req->dest_mn_lp;
     msg->magic = sn_get_magic();
     msg->net_msg_size_bytes = packet_size;
     msg->event_size_bytes = 0;
     msg->local_event_size_bytes = 0;
     msg->event_type = SN_MSG_START;
     msg->is_pull = req->is_pull;
     msg->pull_size = req->pull_size;

     /*Fill in simplenet information*/
     if(is_last_pckt) /* Its the last packet so pass in remote event information*/
      {
       if(req->remote_event_size)
	 {
           msg->event_size_bytes = req->remote_event_size;
           memcpy(tmp_ptr, remote_event, req->remote_event_size);
           tmp_ptr += req->remote_event_size;
	 }
       if(req->self_event_size)
       {
	   msg->local_event_size_bytes = req->self_event_size;
	   memcpy(tmp_ptr, self_event, req->self_event_size);
	   tmp_ptr += req->self_event_size;
       }
      }
     tw_event_send(e_new);
     return xfer_to_nic_time;
}

static void sn_configure()
{
    char intra_socket_config[MAX_NAME_LENGTH];
    char inter_socket_config[MAX_NAME_LENGTH];
    char inter_node_config[MAX_NAME_LENGTH];

    anno_map = codes_mapping_get_lp_anno_map(LP_CONFIG_NM);
    assert(anno_map);
    num_params = N_PARAM_TYPES * (anno_map->num_annos + (anno_map->has_unanno_lp > 0));
    all_params = malloc(num_params * sizeof(*all_params));
    for (int i = 0; i < anno_map->num_annos; i++){
        const char * anno = anno_map->annotations[i].ptr;
        int rc;
        rc = configuration_get_value_int(&config, "PARAMS", "socket_size", anno, &socket_size);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:socket_size@%s", anno);
        }

        rc = configuration_get_value_relpath(&config, "PARAMS", "intra_socket_config",
                anno, intra_socket_config, MAX_NAME_LENGTH);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:intra_socket_config@%s", anno);
        }
        nodes_set_params(intra_socket_config, &all_params[i]);

        rc = configuration_get_value_relpath(&config, "PARAMS", "inter_socket_config",
                anno, inter_socket_config, MAX_NAME_LENGTH);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:inter_socket_config@%s", anno);
        }
        nodes_set_params(inter_socket_config, &all_params[i+1]);

        rc = configuration_get_value_relpath(&config, "PARAMS", "inter_node_config",
                anno, inter_node_config, MAX_NAME_LENGTH);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:inter_node_config@%s", anno);
        }
        nodes_set_params(inter_node_config, &all_params[i+2]);
    }
    if (anno_map->has_unanno_lp > 0){
        int rc;
        rc = configuration_get_value_int(&config, "PARAMS", "socket_size", NULL, &socket_size);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:socket_size");
        }

        rc = configuration_get_value_relpath(&config, "PARAMS", "intra_socket_config",
                NULL, intra_socket_config, MAX_NAME_LENGTH);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:intra_socket_config");
        }
        nodes_set_params(intra_socket_config, &all_params[N_PARAM_TYPES*anno_map->num_annos]);

        rc = configuration_get_value_relpath(&config, "PARAMS", "inter_socket_config",
                NULL, inter_socket_config, MAX_NAME_LENGTH);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:inter_socket_config");
        }
        nodes_set_params(inter_socket_config, &all_params[N_PARAM_TYPES*anno_map->num_annos+1]);

        rc = configuration_get_value_relpath(&config, "PARAMS", "inter_node_config",
                NULL, inter_node_config, MAX_NAME_LENGTH);
        if (rc < 0) {
            tw_error(TW_LOC, "NODES: unable to read PARAMS:inter_node_config");
        }
        nodes_set_params(inter_node_config, &all_params[N_PARAM_TYPES*anno_map->num_annos+2]);
    }

    node_size = codes_mapping_get_lp_count("MODELNET_GRP", 1, "modelnet_simplenet", NULL, 1);

    printf("node_size: %d\n", node_size);
}

void nodes_set_params(const char* config_file, simplenet_param* params) {
    FILE *conf;
    int ret;
    char buffer[512];
    int line = 0;

    printf("NODES using parameters from file %s\n", config_file);

    conf = fopen(config_file, "r");
    if (!conf) {
        perror("fopen");
        assert(0);
    }

    fgets(buffer, 512, conf);
    ret = sscanf(buffer, "%d", &params->k_value);
    line++;
    if (ret != 1) {
        fprintf(stderr, "Malformed line %d in %s\n", line, config_file);
        assert(0);
    }

    fgets(buffer, 512, conf);
    ret = sscanf(buffer, "%d %d", &params->short_limit, &params->eager_limit);
    line++;
    if (ret != 2) {
        fprintf(stderr, "Malformed line %d in %s\n", line, config_file);
        assert(0);
    }

    fgets(buffer, 512, conf);
    ret = sscanf(buffer, "%lf %lf", &params->short_a, &params->short_b);
    line++;
    if (ret != 2) {
        fprintf(stderr, "Malformed line %d in %s\n", line, config_file);
        assert(0);
    }

    fgets(buffer, 512, conf);
    ret = sscanf(buffer, "%lf %lf %lf", &params->eager_a, &params->eager_rcb,
            &params->eager_rci);
    line++;
    if (ret != 3) {
        fprintf(stderr, "Malformed line %d in %s\n", line, config_file);
        assert(0);
    }

    fgets(buffer, 512, conf);
    ret = sscanf(buffer, "%lf %lf %lf", &params->rend_a, &params->rend_rcb,
            &params->rend_rci);
    line++;
    if (ret != 3) {
        fprintf(stderr, "Malformed line %d in %s\n", line, config_file);
        assert(0);
    }

    printf("Parsed %d lines from %s\n", line, config_file);
    printf("Number of communication process pairs (k): %d\n", params->k_value);
    printf("Short limit: %d, eager limit: %d\n", params->short_limit, params->eager_limit);
    printf("Short a (us), b (us/B): %.6lf, %.6lf\n", params->short_a, params->short_b);
    printf("Eager a (us), Rcb (MB/s), Rci (MB/s): %.6lf, %.6lf, %.6lf\n", params->eager_a, params->eager_rcb, params->eager_rci);
    printf("Rendezvous a (us), Rcb (MB/s), Rci (MB/s): %.6lf, %.6lf, %.6lf\n", params->rend_a, params->rend_rcb, params->rend_rci);

    fclose(conf);

    return;
}

static void simplenet_packet_event_rc(tw_lp *sender)
{
    codes_local_latency_reverse(sender);
    return;
}

#if SIMPLENET_DEBUG
void print_msg(sn_message *m){
    printf(" sn:\n  type:%d, magic:%d, src:%lu, dest:%lu, esize:%d, lsize:%d\n",
            m->event_type, m->magic, m->src_gid, m->final_dest_gid,
            m->event_size_bytes, m->local_event_size_bytes);
}
#endif


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
