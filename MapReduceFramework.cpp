//
// Created by iritv on 6/29/2024.
//
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <pthread.h>
#include <vector>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <semaphore.h>
#include <algorithm>
#include <tuple>

/*************************************************************************
 *                           Defines                                     *
 *************************************************************************/
#define SYSTEM_ERROR_MSG "system error: "
#define SUCCESS_RET_VAL 0
#define MASK_31_BIT 0x7FFFFFFF
#define STAGE_SHIFT 62
#define PROCESSED_KEYS_SHIFT 31

/*************************************************************************
 *                           Declarations                                *
 *************************************************************************/
void updateJobStage (JobHandle context, uint64_t processed_keys);
void initNewJobStage (JobHandle context, stage_t new_stage);
void cleanUp (JobHandle *context);
typedef struct ThreadContext ThreadContext;

/*************************************************************************
 *                         Error Handling                                *
 *************************************************************************/
void check_ret_code (int ret_code, const std::string &error_message)
{
  if (ret_code != SUCCESS_RET_VAL)
  {
    std::cout << SYSTEM_ERROR_MSG << error_message << "\n";
    exit (1);
  }
}

/*************************************************************************
 *                           Manage mutex                                *
 *************************************************************************/

void lock_and_validate_mutex (pthread_mutex_t *mutex)
{
  int ret_val = pthread_mutex_lock (mutex);
  check_ret_code (ret_val, "Failed to lock mutex");
}

void unlock_and_validate_mutex (pthread_mutex_t *mutex)
{
  int ret_val = pthread_mutex_unlock (mutex);
  check_ret_code (ret_val, "Failed to unlock mutex");
}

/*************************************************************************
 *                          Struct Typedefs                              *
 *************************************************************************/
typedef struct
{
    const MapReduceClient *client;
    const InputVec *input_vec;
    OutputVec *output_vec;

    std::vector<pthread_t> *threads;
    std::vector<ThreadContext *> *threads_contexts;
    std::vector<IntermediateVec *> *shuffle_vec;

    int number_of_threads;
    bool is_waiting;

    std::atomic<int> atomic_input_counter;
    std::atomic<int> atomic_output_counter;
    std::atomic<int> atomic_inter_pairs_counter;
    std::atomic<int> atomic_out_pairs_counter;
    std::atomic<uint64_t> atomic_curr_job_stage;

    Barrier *barrier;
    pthread_mutex_t map_mutex;
    pthread_mutex_t emit2_mutex;
    pthread_mutex_t emit3_mutex;
    pthread_mutex_t reduce_mutex;
    pthread_mutex_t getState_mutex;

} JobContext;

struct ThreadContext
{
    int tid;
    IntermediateVec *inter_processed_data;
    JobContext *job;
};

/*************************************************************************
 *                            Free Memory                                *
 *************************************************************************/

void freeThreadVectorContent (JobContext *job)
{
  int num_of_threads = job->number_of_threads;
  for (int i = 0; i < num_of_threads; ++i)
  {
    if (job->threads_contexts->at (i) != nullptr)
    {
      job->threads_contexts->at (i)->job = nullptr;
      delete job->threads_contexts->at (i)->inter_processed_data;
      delete job->threads_contexts->at (i);
    }
  }
}

void freeShuffleVectorContent (JobContext *context)
{
  size_t size = context->shuffle_vec->size ();
  for (size_t i = 0; i < size; i++)
  {
    delete context->shuffle_vec->at (i);
  }
}

void destroyMutex (JobContext *context)
{
  pthread_mutex_destroy (&context->map_mutex);
  pthread_mutex_destroy (&context->emit2_mutex);
  pthread_mutex_destroy (&context->emit3_mutex);
  pthread_mutex_destroy (&context->reduce_mutex);
  pthread_mutex_destroy (&context->getState_mutex);
}

void cleanUp (JobContext *context)
{
  destroyMutex (context);
  delete context->barrier;

  //Delete Vectors
  freeShuffleVectorContent (context);
  freeThreadVectorContent (context);
  delete context->threads_contexts;
  delete context->threads;
  delete context->shuffle_vec;
  delete context;
}

/*************************************************************************
 *                        JobContext Management                          *
 *************************************************************************/

JobContext *createJobContext (const MapReduceClient &client,
                              const InputVec &inputVec, OutputVec &outputVec,
                              int multiThreadLevel)
{

  auto *job_context = new JobContext ();

  job_context->client = &client;
  job_context->input_vec = &inputVec;
  job_context->output_vec = &outputVec;

  job_context->is_waiting = false;
  job_context->number_of_threads = multiThreadLevel;

  job_context->atomic_input_counter = 0;
  job_context->atomic_output_counter = 0;
  job_context->atomic_inter_pairs_counter = 0;
  job_context->atomic_out_pairs_counter = 0;

  pthread_mutex_init (&job_context->map_mutex, nullptr);
  pthread_mutex_init (&job_context->emit2_mutex, nullptr);
  pthread_mutex_init (&job_context->emit3_mutex, nullptr);
  pthread_mutex_init (&job_context->reduce_mutex, nullptr);
  pthread_mutex_init (&job_context->getState_mutex, nullptr);

  job_context->barrier = new Barrier (multiThreadLevel);

  job_context->threads = new std::vector<pthread_t> (multiThreadLevel);
  job_context->threads_contexts = new std::vector<ThreadContext *> ();
  job_context->shuffle_vec = new std::vector<IntermediateVec *> ();

  initNewJobStage (job_context, UNDEFINED_STAGE);

  return job_context;
}

void createThreadVector (JobContext *job)
{
  int num_of_threads = job->number_of_threads;
  for (int i = 0; i < num_of_threads; i++)
  {
    auto *thread_context = new ThreadContext ();
    thread_context->tid = i;
    thread_context->job = job;
    thread_context->inter_processed_data = new IntermediateVec ();
    job->threads_contexts->push_back (thread_context);
  }
}

// Function to get JobContext from JobHandle
JobContext *getJobContext (JobHandle jobHandle)
{
  return static_cast<JobContext *>(jobHandle);
}

std::tuple<ThreadContext *, JobContext *> extractArguments (void *args)
{
  auto *thread_context = static_cast<ThreadContext *>(args);
  auto *context = thread_context->job;
  return std::make_tuple (thread_context, context);
}

/*************************************************************************
 *                        Manage Job State                               *
 *************************************************************************/
void initNewJobStage (JobHandle context, stage_t new_stage)
{
  JobContext *job_context = getJobContext (context);
  uint64_t full_keys = 0;
  if (new_stage == UNDEFINED_STAGE)
  {
    full_keys = 0;
  }
  else if (new_stage == MAP_STAGE)
  {
    full_keys = job_context->input_vec->size ();
  }
  else
  {
    full_keys = job_context->atomic_inter_pairs_counter.load ();
  }
  uint64_t stage =
      (uint64_t) new_stage << STAGE_SHIFT | full_keys << PROCESSED_KEYS_SHIFT;
  job_context->atomic_curr_job_stage.store (stage);
}

void updateJobStage (JobHandle context, uint64_t processed_keys)
{
  JobContext *job_context = getJobContext (context);
  uint64_t current_stage = job_context->atomic_curr_job_stage.load ();
  uint64_t total_bits = (current_stage >> PROCESSED_KEYS_SHIFT) & MASK_31_BIT;
  if (processed_keys >= total_bits)
  {
    processed_keys = total_bits;
  }
  uint64_t processed_bits = (processed_keys);
  uint64_t new_stage = (current_stage & ~MASK_31_BIT) | processed_bits;
  job_context->atomic_curr_job_stage.store (new_stage);
}

/*************************************************************************
 *                        Map Implementation                             *
 *************************************************************************/

void runMapPhase (void *args)
{
  ThreadContext *thread_context;
  JobContext *context;
  std::tie (thread_context, context) = extractArguments (args);

  int input_vec_size = (int) context->input_vec->size ();
  int old_value = 0;
  while (true)
  {
    old_value = context->atomic_input_counter.fetch_add (1);
    if (old_value >= input_vec_size)
    {
      break;
    }
    lock_and_validate_mutex (&context->map_mutex);
    updateJobStage (context, context->atomic_input_counter.load ());
    unlock_and_validate_mutex (&context->map_mutex);
    const InputPair &pair = (*context->input_vec)[old_value];
    context->client->map (pair.first, pair.second, thread_context);
  }
}

/*************************************************************************
 *                        Shuffle Implementation                         *
 *************************************************************************/

bool keysAreEqual (const K2 *key1, const K2 *key2)
{
  return !(*key1 < *key2) && !(*key2 < *key1);
}

K2 *getMaxKey (JobContext *context)
{
  K2 *max_key = nullptr;

  for (int i = 0; i < context->number_of_threads; i++)
  {
    IntermediateVec t_vec = *context->threads_contexts->at (i)
        ->inter_processed_data;
    while (!t_vec.empty ())
    {
      K2 *curr_key = t_vec.back().first;
      if (max_key == nullptr || *max_key < *curr_key)
      {
        max_key = curr_key;
      }
      t_vec.pop_back ();
    }
  }
  return max_key;
}

void runShufflePhase (void *args)
{

  ThreadContext *curren_thread;
  JobContext *context;
  std::tie (curren_thread, context) = extractArguments (args);

  if (curren_thread->tid != 0)
  { return; }

  initNewJobStage (context, SHUFFLE_STAGE);
  uint64_t processed_keys = 0;

  while (true)
  {
    K2 *max_key = getMaxKey(context);
    if (max_key == nullptr)
    {
      break;
    }
    auto *max_key_vec = new IntermediateVec;
    for (int i = 0; i < context->number_of_threads; ++i)
    {
      ThreadContext *t = context->threads_contexts->at (i);
      IntermediateVec *t_vec = t->inter_processed_data;
      while (!t_vec->empty() && keysAreEqual(max_key, t_vec->back().first))
      {
        max_key_vec->push_back(t_vec->back());
        processed_keys++;
        t_vec->pop_back();
      }
      updateJobStage (context, processed_keys);
    }
    context->shuffle_vec->push_back (max_key_vec);
  }
  initNewJobStage (context, REDUCE_STAGE);
}

/*************************************************************************
 *                        Reduce Implementation                          *
 *************************************************************************/
void *runReducePhase (void *args)
{

  ThreadContext *thread_context;
  JobContext *context;
  std::tie (thread_context, context) = extractArguments (args);

  int shuffle_vec_size = (int) context->shuffle_vec->size ();
  int old_value = 0;

  while (true)
  {
    old_value = context->atomic_output_counter.fetch_add (1);
    if (old_value >= shuffle_vec_size)
    {
      break;
    }
    const IntermediateVec *vec_to_reduce = context->shuffle_vec->at (old_value);
    context->atomic_out_pairs_counter.fetch_add ((int) vec_to_reduce->size ());
    lock_and_validate_mutex (&context->reduce_mutex);
    updateJobStage (context, context->atomic_out_pairs_counter.load ());
    unlock_and_validate_mutex (&context->reduce_mutex);
    context->client->reduce (vec_to_reduce, thread_context);
  }
}

/*************************************************************************
 *                        MapReduce Implementation                         *
 *************************************************************************/
//Comparator for Sort Phase
bool compare_keys (const IntermediatePair &a, const IntermediatePair &b)
{
  return (*(a.first) < *(b.first));
}

void *runMapReduceAlgorithm (void *args)
{
  ThreadContext *thread_context;
  JobContext *context;
  std::tie (thread_context, context) = extractArguments (args);


  //Run Map phase
  runMapPhase (args);

  // Each thread sorts its own intermediate_data vector
  IntermediateVec &thread_inter_vec = *thread_context->inter_processed_data;
  std::sort (thread_inter_vec.begin (), thread_inter_vec.end (),
             compare_keys);

  //make sure that all threads finished sorting their intermediate vectors
  thread_context->job->barrier->barrier ();

  //run shuffle only on thread 0
  runShufflePhase (args);

  //make sure that all threads will start together the reduce phase
  thread_context->job->barrier->barrier ();
  runReducePhase (args);

  return nullptr;

}

/*************************************************************************
 *                            API Framework                              *
 *************************************************************************/

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  JobContext *job_context = createJobContext (client, inputVec, outputVec, multiThreadLevel);
  createThreadVector (job_context);

  //Initialize Job stage to MAP before starting
  initNewJobStage (job_context, MAP_STAGE);

  // run map reduce algorithm on threads 0 to multiThreadLevel-1
  for (int i = 0; i < multiThreadLevel; i++)
  {
    int ret_val = pthread_create (&job_context->threads->at (i),
                                  nullptr,
                                  runMapReduceAlgorithm,
                                  job_context->threads_contexts->at (i));

    check_ret_code (ret_val, "Failed to create threads");
  }
  return static_cast<JobHandle>(job_context);
}

void emit2 (K2 *key, V2 *value, void *context)
{
  ThreadContext *thread_context;
  JobContext *job_context;
  std::tie (thread_context, job_context) = extractArguments (context);

  lock_and_validate_mutex (&job_context->emit2_mutex);
  thread_context->inter_processed_data->push_back ({key, value});
  job_context->atomic_inter_pairs_counter.fetch_add (1);
  unlock_and_validate_mutex (&job_context->emit2_mutex);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  ThreadContext *thread_context;
  JobContext *job_context;
  std::tie (thread_context, job_context) = extractArguments (context);

  lock_and_validate_mutex (&job_context->emit3_mutex);
  job_context->output_vec->push_back ({key, value});
  unlock_and_validate_mutex (&job_context->emit3_mutex);
}

void waitForJob (JobHandle job)
{
  JobContext *job_context = getJobContext (job);
  if (job_context->is_waiting)
  {
    return;
  }
  for (int i = 0; i < job_context->number_of_threads; i++)
  {
    pthread_t thread_id = job_context->threads->at (i);
    int ret_val = pthread_join ((pthread_t) thread_id, nullptr);
    check_ret_code (ret_val, "Failed calling pthread_join");
  }
  job_context->is_waiting = true;
}

void getJobState (JobHandle job, JobState *state)
{
  JobContext *job_context = getJobContext (job);

  lock_and_validate_mutex (&job_context->getState_mutex);
  uint64_t job_state = job_context->atomic_curr_job_stage.load ();
  uint64_t stage_number = job_state >> STAGE_SHIFT;
  state->stage = static_cast<stage_t>(stage_number);
  if (stage_number == 0)
  {
    state->percentage = 0;
  }
  else
  {
    uint64_t processed_jobs = job_state & MASK_31_BIT;
    uint64_t total_jobs =
        (job_state >> PROCESSED_KEYS_SHIFT) & MASK_31_BIT;
    state->percentage = 100 * (float) (processed_jobs) / (float) total_jobs;
  }
  unlock_and_validate_mutex (&job_context->getState_mutex);
}

void closeJobHandle (JobHandle job)
{
  waitForJob (job);
  JobContext *job_context = getJobContext (job);

  //Delete sync managements
  destroyMutex (job_context);
  delete job_context->barrier;

  //Delete Vectors
  freeShuffleVectorContent (job_context);
  freeThreadVectorContent (job_context);
  delete job_context->threads_contexts;
  delete job_context->threads;
  delete job_context->shuffle_vec;
  delete job_context;
  job = nullptr;
}
