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
#include "codes/codes_mapping.h"
#include "codes/codes.h"

#define CATEGORY_NAME_MAX 16
#define CATEGORY_MAX 12

/*Define simplenet data types and structs*/
typedef struct sn_message sn_message;
typedef struct sn_state sn_state;

/* types of events that will constitute triton requests */
enum sn_event_type 
{
    MSG_READY = 1,  /* sender has transmitted msg to receiver */
    MSG_START,      /* initiate a transmission */
};


struct sn_state
{
    /* next idle times for network card, both inbound and outbound */
    tw_stime net_send_next_idle;
    tw_stime net_recv_next_idle;
    struct mn_stats sn_stats_array[CATEGORY_MAX];
};

struct sn_message
{
    int magic; /* magic number */
    enum sn_event_type event_type;
    tw_lpid src_gid; /* who transmitted this msg? */
    tw_lpid final_dest_gid; /* who is eventually targetted with this msg? */
    int net_msg_size_bytes;     /* size of modeled network message */
    int event_size_bytes;     /* size of simulator event message that will be tunnelled to destination */
    int local_event_size_bytes;     /* size of simulator event message that delivered locally upon local completion */
    char category[CATEGORY_NAME_MAX]; /* category for communication */

    /* for reverse computation */
    tw_stime net_send_next_idle_saved;
    tw_stime net_recv_next_idle_saved;
};
/* net startup cost, ns */
static double global_net_startup_ns = 0;
/* net bw, MB/s */
static double global_net_bw_mbs = 0;

static int sn_magic = 0;

/* returns a pointer to the lptype struct to use for simplenet LPs */
static const tw_lptype* sn_get_lp_type(void);

/* set model parameters:
 *
 * - net_startup_ns: network startup cost in ns.
 * - net_bs_mbs: network bandwidth in MiB/s (megabytes per second).
 */
static void sn_set_params(double net_startup_ns, double net_bw_mbs);

/* retrieve the size of the portion of the event struct that is consumed by
 * the simplenet module.  The caller should add this value to the size of
 * its own event structure to get the maximum total size of a message.
 */
static int sn_get_msg_sz(void);

/* Returns the simplenet magic number */
static int sn_get_magic();

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
/* Modelnet interface events */
/* sets up the simplenet parameters through modelnet interface */
static void sn_setup(const void* net_params);

/* Issues a simplenet packet event call */
static void simplenet_packet_event(
     char* category, 
     tw_lpid final_dest_lp, 
     int packet_size, 
     int remote_event_size, 
     const void* remote_event, 
     int self_event_size,
     const void* self_event,
     tw_lp *sender,
     int is_last_pckt);
static void simplenet_packet_event_rc(tw_lp *sender);

static void simplenet_packet_event_rc(tw_lp *sender);

static void sn_report_stats();

static tw_lpid sn_find_local_device(tw_lp *sender);

/* data structure for model-net statistics */
struct model_net_method simplenet_method =
{
    .method_name = "simplenet",
    .mn_setup = sn_setup,
    .model_net_method_packet_event = simplenet_packet_event,
    .model_net_method_packet_event_rc = simplenet_packet_event_rc,
    .mn_get_lp_type = sn_get_lp_type,
    .mn_get_msg_sz = sn_get_msg_sz,
    .mn_report_stats = sn_report_stats,
    .model_net_method_find_local_device = sn_find_local_device,
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
     (event_f) sn_event,
     (revent_f) sn_rev_event,
     (final_f) sn_finalize,
     (map_f) codes_mapping,
     sizeof(sn_state),
};

static tw_stime rate_to_ns(unsigned int bytes, double MB_p_s);
static void handle_msg_ready_rev_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp);
static void handle_msg_ready_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp);
static void handle_msg_start_rev_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp);
static void handle_msg_start_event(
    sn_state * ns,
    tw_bf * b,
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

/* lets caller specify model parameters to use */
static void sn_set_params(double net_startup_ns, double net_bw_mbs)
{
    assert(net_startup_ns > 0);
    assert(net_bw_mbs > 0);
    global_net_startup_ns = net_startup_ns;
    global_net_bw_mbs = net_bw_mbs;
    
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

    bj_hashlittle2("simplenet", strlen("simplenet"), &h1, &h2);
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
    assert(m->magic == sn_magic);

    switch (m->event_type)
    {
        case MSG_START:
            handle_msg_start_event(ns, b, m, lp);
            break;
        case MSG_READY:
            handle_msg_ready_event(ns, b, m, lp);
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
    assert(m->magic == sn_magic);

    switch (m->event_type)
    {
        case MSG_START:
            handle_msg_start_rev_event(ns, b, m, lp);
            break;
        case MSG_READY:
            handle_msg_ready_rev_event(ns, b, m, lp);
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

/* convert MiB/s and bytes to ns */
static tw_stime rate_to_ns(unsigned int bytes, double MB_p_s)
{
    tw_stime time;

    /* bytes to MB */
    time = ((double)bytes)/(1024.0*1024.0);
    /* MB to s */
    time = time / MB_p_s;
    /* s to ns */
    time = time * 1000.0 * 1000.0 * 1000.0;

    return(time);
}

/* reverse computation for msg ready event */
static void handle_msg_ready_rev_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp)
{
    struct mn_stats* stat;

    ns->net_recv_next_idle = m->net_recv_next_idle_saved;
    
    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->recv_count--;
    stat->recv_bytes -= m->net_msg_size_bytes;
    stat->recv_time -= rate_to_ns(m->net_msg_size_bytes, global_net_bw_mbs);

    return;
}

/* handler for msg ready event.  This indicates that a message is available
 * to recv, but we haven't checked to see if the recv queue is available yet
 */
static void handle_msg_ready_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp)
{
    tw_stime recv_queue_time = 0;
    tw_event *e_new;
    sn_message *m_new;
    struct mn_stats* stat;

    //printf("handle_msg_ready_event(), lp %llu.\n", (unsigned long long)lp->gid);
    /* add statistics */
    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->recv_count++;
    stat->recv_bytes += m->net_msg_size_bytes;
    stat->recv_time += rate_to_ns(m->net_msg_size_bytes, global_net_bw_mbs);

    /* are we available to recv the msg? */
    /* were we available when the transmission was started? */
    if(ns->net_recv_next_idle > tw_now(lp))
        recv_queue_time += ns->net_recv_next_idle - tw_now(lp);

    /* calculate transfer time based on msg size and bandwidth */
    recv_queue_time += rate_to_ns(m->net_msg_size_bytes, global_net_bw_mbs);

    /* bump up input queue idle time accordingly */
    m->net_recv_next_idle_saved = ns->net_recv_next_idle;
    ns->net_recv_next_idle = recv_queue_time + tw_now(lp);

    /* copy only the part of the message used by higher level */
    if(m->event_size_bytes)
    {
      /* schedule event to final destination for when the recv is complete */
//      printf("\n Remote message to LP %d ", m->final_dest_gid); 

      e_new = codes_event_new(m->final_dest_gid, recv_queue_time, lp);
      m_new = tw_event_data(e_new);
      char* tmp_ptr = (char*)m;
      tmp_ptr += sn_get_msg_sz();
      memcpy(m_new, tmp_ptr, m->event_size_bytes);
      tw_event_send(e_new);
    }

    return;
}

/* reverse computation for msg start event */
static void handle_msg_start_rev_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp)
{
    ns->net_send_next_idle = m->net_send_next_idle_saved;

    if(m->local_event_size_bytes > 0)
    {
        codes_local_latency_reverse(lp);
    }

    mn_stats* stat;
    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->send_count--;
    stat->send_bytes -= m->net_msg_size_bytes;
    stat->send_time -= global_net_startup_ns + rate_to_ns(m->net_msg_size_bytes, global_net_bw_mbs);

    return;
}

/* handler for msg start event; this indicates that the caller is trying to
 * transmit a message through this NIC
 */
static void handle_msg_start_event(
    sn_state * ns,
    tw_bf * b,
    sn_message * m,
    tw_lp * lp)
{
    tw_event *e_new;
    sn_message *m_new;
    tw_stime send_queue_time = 0;
    mn_stats* stat;
    int mapping_grp_id, mapping_type_id, mapping_rep_id, mapping_offset;
    tw_lpid dest_id;
    char lp_type_name[MAX_NAME_LENGTH], lp_group_name[MAX_NAME_LENGTH];
    int total_event_size;

    total_event_size = sn_get_msg_sz() + m->event_size_bytes + m->local_event_size_bytes;

    //printf("handle_msg_start_event(), lp %llu.\n", (unsigned long long)lp->gid);
    /* add statistics */
    stat = model_net_find_stats(m->category, ns->sn_stats_array);
    stat->send_count++;
    stat->send_bytes += m->net_msg_size_bytes;
    stat->send_time += global_net_startup_ns + rate_to_ns(m->net_msg_size_bytes, global_net_bw_mbs);
    if(stat->max_event_size < total_event_size)
        stat->max_event_size = total_event_size;

    /* calculate send time stamp */
    send_queue_time = global_net_startup_ns; /* net msg startup cost */
    /* bump up time if the NIC send queue isn't idle right now */
    if(ns->net_send_next_idle > tw_now(lp))
        send_queue_time += ns->net_send_next_idle - tw_now(lp);

    /* move the next idle time ahead to after this transmission is
     * _complete_ from the sender's perspective 
     */ 
    m->net_send_next_idle_saved = ns->net_send_next_idle;
    ns->net_send_next_idle = send_queue_time + tw_now(lp) +
        rate_to_ns(m->net_msg_size_bytes, global_net_bw_mbs);


    /* create new event to send msg to receiving NIC */
    codes_mapping_get_lp_info(m->final_dest_gid, lp_group_name, &mapping_grp_id, &mapping_type_id, lp_type_name, &mapping_rep_id, &mapping_offset);
    codes_mapping_get_lp_id(lp_group_name, "modelnet_simplenet", mapping_rep_id , mapping_offset, &dest_id); 

//    printf("\n msg start sending to %d ", dest_id);
    e_new = codes_event_new(dest_id, send_queue_time, lp);
    m_new = tw_event_data(e_new);

    /* copy entire previous message over, including payload from user of
     * this module
     */
    memcpy(m_new, m, m->event_size_bytes + sn_get_msg_sz());
    m_new->event_type = MSG_READY;
    
    tw_event_send(e_new);

    /* if there is a local event to handle, then create an event for it as
     * well
     */
    if(m->local_event_size_bytes > 0)
    {
        char* local_event;

        e_new = codes_event_new(m->src_gid, send_queue_time+codes_local_latency(lp), lp);
        m_new = tw_event_data(e_new);

         local_event = (char*)m;
         local_event += sn_get_msg_sz() + m->event_size_bytes;         	 
        /* copy just the local event data over */
        memcpy(m_new, local_event, m->local_event_size_bytes);
        tw_event_send(e_new);
    }
    return;
}

/* Model-net function calls */

/*This method will serve as an intermediate layer between simplenet and modelnet. 
 * It takes the packets from modelnet layer and calls underlying simplenet methods*/
static void simplenet_packet_event(
		char* category,
		tw_lpid final_dest_lp,
		int packet_size,
		int remote_event_size,
		const void* remote_event,
		int self_event_size,
		const void* self_event,
		tw_lp *sender,
		int is_last_pckt)
{
     tw_event * e_new;
     tw_stime xfer_to_nic_time;
     sn_message * msg;
     tw_lpid dest_id;
     char* tmp_ptr;
     char lp_type_name[MAX_NAME_LENGTH], lp_group_name[MAX_NAME_LENGTH];

     int mapping_grp_id, mapping_rep_id, mapping_type_id, mapping_offset;
     codes_mapping_get_lp_info(sender->gid, lp_group_name, &mapping_grp_id, &mapping_type_id, lp_type_name, &mapping_rep_id, &mapping_offset);
     codes_mapping_get_lp_id(lp_group_name, "modelnet_simplenet", mapping_rep_id, mapping_offset, &dest_id);

     xfer_to_nic_time = codes_local_latency(sender);
     e_new = codes_event_new(dest_id, xfer_to_nic_time, sender);
     msg = tw_event_data(e_new);
     strcpy(msg->category, category);
     msg->final_dest_gid = final_dest_lp;
     msg->src_gid = sender->gid;
     msg->magic = sn_get_magic();
     msg->net_msg_size_bytes = packet_size;
     msg->event_size_bytes = 0;
     msg->local_event_size_bytes = 0;
     msg->event_type = MSG_START;

     tmp_ptr = (char*)msg;
     tmp_ptr += sn_get_msg_sz();
      
    //printf("\n Sending to LP %d msg magic %d ", (int)dest_id, sn_get_magic()); 
     /*Fill in simplenet information*/     
     if(is_last_pckt) /* Its the last packet so pass in remote event information*/
      {
       if(remote_event_size)
	 {
           msg->event_size_bytes = remote_event_size;
           memcpy(tmp_ptr, remote_event, remote_event_size);
           tmp_ptr += remote_event_size;
	 }
       if(self_event_size)
       {
	   msg->local_event_size_bytes = self_event_size;
	   memcpy(tmp_ptr, self_event, self_event_size);
	   tmp_ptr += self_event_size;
       }
      // printf("\n Last packet size: %d ", sn_get_msg_sz() + remote_event_size + self_event_size);
      }
     tw_event_send(e_new);
}

static void sn_setup(const void* net_params)
{
  simplenet_param* sn_param = (simplenet_param*)net_params;
  sn_set_params(sn_param->net_startup_ns, sn_param->net_bw_mbps); 
  //printf("\n Bandwidth setup %lf %lf ", sn_param->net_startup_ns, sn_param->net_bw_mbps);
}

static void simplenet_packet_event_rc(tw_lp *sender)
{
    codes_local_latency_reverse(sender);
    return;
}

static tw_lpid sn_find_local_device(tw_lp *sender)
{
     char lp_type_name[MAX_NAME_LENGTH], lp_group_name[MAX_NAME_LENGTH];
     int mapping_grp_id, mapping_rep_id, mapping_type_id, mapping_offset;
     tw_lpid dest_id;

     codes_mapping_get_lp_info(sender->gid, lp_group_name, &mapping_grp_id, &mapping_type_id, lp_type_name, &mapping_rep_id, &mapping_offset);
     codes_mapping_get_lp_id(lp_group_name, "modelnet_simplenet", mapping_rep_id, mapping_offset, &dest_id);

    return(dest_id);
}


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
