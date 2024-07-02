//
// Created by iritv on 6/29/2024.
//
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <pthread.h>
#include <csignal>
#include <vector>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <semaphore.h>
#include <algorithm>

/*************************************************************************
 *                           Defines                                     *
 *************************************************************************/
#define SYSTEM_ERROR_MSG "system error: "
#define SUCCESS_RET_VAL 0

/*************************************************************************
 *                        JobContext Management                          *
 *************************************************************************/
void update_job_state(JobHandle context, stage_t new_stage, int
processed_keys);
typedef struct ThreadContext ThreadContext;


typedef struct
{
    const MapReduceClient *client;
    const InputVec *input_vec;
    OutputVec *output_vec;
    std::vector<ThreadContext*>* threads_contexts;
    std::vector<IntermediateVec> intermediate_data;
    std::vector<IntermediateVec*> shuffle_vec;
    std::vector<pthread_t>* threads;
    int number_of_threads;
    bool is_waiting;
    std::atomic<int> atomic_input_counter;
    std::atomic<int> atomic_output_counter;
    std::atomic<uint64_t> job_state;
    std::atomic<int> intermediate_pairs_counter;
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
    JobContext *job;
};


JobContext *createJobContext (const MapReduceClient &client,
                              const InputVec &inputVec, OutputVec &outputVec,
                              int multiThreadLevel)
{
  auto *job_context = (JobContext *) (malloc (sizeof (JobContext)));
  if (job_context == nullptr)
  {
    std::cout << SYSTEM_ERROR_MSG << "Failed to allocate memory." << std::endl;
    exit (1);
  }

  job_context->client = &client;
  job_context->input_vec = &inputVec;
  job_context->output_vec = &outputVec;
  job_context->number_of_threads = multiThreadLevel;
  job_context->is_waiting = false;
  job_context->atomic_input_counter = 0;
  job_context->atomic_output_counter = 0;

  pthread_mutex_init (&job_context->map_mutex, nullptr);
  pthread_mutex_init (&job_context->emit2_mutex, nullptr);
  pthread_mutex_init (&job_context->emit3_mutex, nullptr);
  pthread_mutex_init (&job_context->reduce_mutex, nullptr);

  job_context->barrier = new Barrier (multiThreadLevel);

  // Resize vectors
  job_context->threads_contexts = new std::vector<ThreadContext*>();
  job_context->intermediate_data.resize (multiThreadLevel);
  job_context->threads = new std::vector<pthread_t>(multiThreadLevel);

  update_job_state (job_context, UNDEFINED_STAGE, 0);

  return job_context;
}

void freeThreadVector(JobContext* job)
{
  int num_of_threads = job->number_of_threads;
  for (int i = 0; i < num_of_threads; ++i)
  {
      if(job->threads_contexts->at (i) != nullptr){
        job->threads_contexts->at (i)->job = nullptr;
        free (job->threads_contexts->at (i));
      }
  }
}

void freeShuffleVector (JobContext *context){
  int shuflle_size = context->shuffle_vec.size();
  for(int i=0; i<shuflle_size; i++){
    delete context->shuffle_vec.at(i);
  }
}


void createThreadVector(JobContext* job)
{
  int num_of_threads = job->number_of_threads;
  for (int i = 0; i < num_of_threads; i++)
  {
    ThreadContext * thread_context = (ThreadContext *)malloc(sizeof
        (ThreadContext ));
    if(thread_context== nullptr)
    {
      std::cout << SYSTEM_ERROR_MSG << "Failed to allocate memory while "
                                       "creating thread struct" << std::endl;
      freeThreadVector(job);
      exit(1);
    }
    thread_context->tid = i;
    thread_context->job = job;
    job->threads_contexts->push_back(thread_context);
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

// Function to get JobContext from JobHandle
JobContext *getJobContext (JobHandle jobHandle)
{
  return static_cast<JobContext *>(jobHandle);
}

/*************************************************************************
 *                         Error Handling                                *
 *************************************************************************/
void check_ret_code (int ret_code, std::string &error_message)
{
  if (ret_code != SUCCESS_RET_VAL)
  {
    std::cout << SYSTEM_ERROR_MSG << error_message << std::endl;
    exit (1);
  }
}

/*************************************************************************
 *                        Helper Functions                               *
 *************************************************************************/

void update_job_state(JobHandle context, stage_t new_stage, int
processed_keys)
{
  JobContext* job_context = getJobContext (context);
  uint64_t state;
  uint64_t full_keys;
  switch (new_stage)
  {
    case MAP_STAGE:
      full_keys = job_context->input_vec->size();
      break;
    case SHUFFLE_STAGE:
      full_keys = job_context->intermediate_pairs_counter.load();
      break;
    case REDUCE_STAGE:
      full_keys = job_context->shuffle_vec.size();
      break;
    case UNDEFINED_STAGE:
      full_keys = job_context->input_vec->size();
  }
  state = new_stage << 62  | processed_keys << 31 | full_keys;
  job_context->job_state.store (state);
}



void lock_and_validate_mutex(pthread_mutex_t* mutex)
{
  int ret_val = pthread_mutex_lock(mutex);
  check_ret_code (ret_val, (std::string &) "Failed to lock mutex");
}

void unlock_and_validate_mutex(pthread_mutex_t* mutex)
{
  int ret_val = pthread_mutex_unlock(mutex);
  check_ret_code (ret_val, (std::string &) "Failed to unlock mutex");
}

/*************************************************************************
 *                        Map Implementation                             *
 *************************************************************************/

void runMapPhase (void *args)
{
  auto *thread_context = static_cast<ThreadContext *>(args);
  auto * context = thread_context->job;
  int input_vec_size = context->input_vec->size ();
  int old_value = context->atomic_input_counter.fetch_add (1);
  while (old_value < input_vec_size)
  {
    lock_and_validate_mutex (&context->map_mutex);
    //TODO check if the counter works
    update_job_state (context,MAP_STAGE, old_value + 1);
    unlock_and_validate_mutex (&context->map_mutex);
    const InputPair &pair = (*context->input_vec)[old_value];
    context->client->map (pair.first, pair.second, thread_context);
    old_value = context->atomic_input_counter.fetch_add (1);
  }
}

/*************************************************************************
 *                        Shuffle Implementation                         *
 *************************************************************************/

bool keysAreEqual (const K2 *key1, const K2 *key2)
{
  return !(*key1 < *key2) && !(*key2 < *key1);
}

int find_key(void* shuffle_vec, K2* key)
{
  int index = 0;
  auto vec = static_cast<std::vector<IntermediateVec*>*>(shuffle_vec);
   for(auto & intermediate: *vec){
     auto pair = intermediate->back();
     if(keysAreEqual (pair.first, key)){
       return index;
     }
     index++;
   }
   return -1;
}

void runShufflePhase (void *args)
{
  int processed_keys = 0;
  auto *thread_context = static_cast<ThreadContext *>(args);
  auto * context = thread_context->job;
  if(thread_context->tid != 0){return;}

  // Process each thread's intermediate data
  for (int i = 0; i < context->number_of_threads; ++i)
  {
    auto &intermediate_vec = context->intermediate_data[i];
    while (!intermediate_vec.empty ())
    {
      IntermediatePair intermediate_pair = intermediate_vec.back ();
      K2* current_key = intermediate_pair.first;
      int index = find_key(&context->shuffle_vec,current_key);
      if(index != -1)
      {
        context->shuffle_vec.at(index)->push_back(intermediate_pair);
      }
      else
      {
        IntermediateVec *current_key_vec = new IntermediateVec;
        current_key_vec->push_back (intermediate_pair);
        context->shuffle_vec.push_back(current_key_vec);
      }
      processed_keys++;
      update_job_state (context, SHUFFLE_STAGE, processed_keys);
      intermediate_vec.pop_back ();
    }
  }
}

/*************************************************************************
 *                        Reduce Implementation                          *
 *************************************************************************/
void *runReducePhase (void *args)
{
  int processed_vectors = 0;
  auto *thread_context = static_cast<ThreadContext *>(args);
  auto * context = thread_context->job;
  int shuffle_vec_size = context->shuffle_vec.size ();
  int old_value = context->atomic_output_counter.fetch_add (1);
  while (old_value < shuffle_vec_size)
  {
    lock_and_validate_mutex (&context->reduce_mutex);
    processed_vectors ++;
    update_job_state (context, REDUCE_STAGE, processed_vectors);
    unlock_and_validate_mutex (&context->reduce_mutex);
    const IntermediateVec* vec_to_reduce = context->shuffle_vec[old_value];
    context->client->reduce (vec_to_reduce, thread_context);
    old_value = context->atomic_output_counter.fetch_add (1);
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
  auto *thread_context = static_cast<ThreadContext *>(args);
  int thread_id = thread_context->tid;
  auto* context = thread_context->job;


  //Run Map phase
  runMapPhase (args);

  // Each thread sorts its own intermediate_data vector
  std::sort (context->intermediate_data[thread_id].begin(),
             context->intermediate_data[thread_id].end(),
             compare_keys);

  //make sure that all threads finished sorting their intermediate vectors
  thread_context->job->barrier->barrier ();

  //run shuffle only on thread 0
  runShufflePhase (args);

  //make sure that all threads will start together the reduce phase
  thread_context->job->barrier->barrier ();
  runReducePhase(args);

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
  // run map reduce algorithm on threads 0 to multiThreadLevel-1
  for (int i = 0; i < multiThreadLevel; i++)
  {
    int ret_val = pthread_create (&(job_context->threads->at(i)),
                                  nullptr,
                                  runMapReduceAlgorithm,
                                  job_context->threads_contexts->at (i));

    check_ret_code (ret_val, (std::string &) "Failed to create threads");
  }
  return static_cast<JobHandle>(job_context);
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto *thread_context = static_cast<ThreadContext *>(context);
  int thread_id = thread_context->tid;
  auto* job_context = thread_context->job;
  auto &ref_intermediate_data = job_context->intermediate_data;
  lock_and_validate_mutex (&job_context->emit2_mutex);
  IntermediatePair pair2(key,value);
  ref_intermediate_data.at(thread_id).push_back(pair2);
  job_context->intermediate_pairs_counter.fetch_add (1);
  unlock_and_validate_mutex (&job_context->emit2_mutex);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto *thread_context = static_cast<ThreadContext *>(context);
  auto* job_context = thread_context->job;
  lock_and_validate_mutex (&job_context->emit3_mutex);
  OutputPair pair3 (key,value);
  job_context->output_vec->push_back(pair3);
  unlock_and_validate_mutex (&job_context->emit3_mutex);
}

void waitForJob (JobHandle job)
{
  JobContext *job_context = getJobContext (job);
  if(job_context->is_waiting){
    return;
  }
  for(int i =0; i< job_context->number_of_threads; i++){
    pthread_t thread_id = job_context->threads->at(i);
//    std::cout<<job_context->threads->at (i) <<std::endl;
    int ret_val = pthread_join (thread_id, nullptr);
    check_ret_code (ret_val, (std::string &) "Failed calling pthread_join");
  }
//  for (pthread_t thread: *job_context->threads)
//  {
//    std::cout<<thread <<std::endl;
//    int ret_val = pthread_join (thread, nullptr);
//    check_ret_code (ret_val, (std::string &) "Failed calling pthread_join");
//  }
  job_context->is_waiting= true;
}

void getJobState (JobHandle job, JobState *state)
{
  JobContext *job_context = getJobContext (job);
  lock_and_validate_mutex (&job_context->getState_mutex);
  uint64_t job_state = job_context->job_state.load();
  uint64_t stage_number = job_state >> 62;
  state->stage = static_cast<stage_t>(stage_number);
  if(stage_number == 0){
    state->percentage = 0;
  }
  else{
    uint64_t total_jobs = job_state & 0x7FFFFFFF;
    uint64_t processed_jobs = (job_state >>31) & 0x7FFFFFFF;
    state->percentage = (float )(100 * (processed_jobs / total_jobs));
  }
  unlock_and_validate_mutex (&job_context->getState_mutex);
}

void closeJobHandle (JobHandle job)
{
  waitForJob (job);
  JobContext *job_context = getJobContext (job);
  destroyMutex (job_context);
  delete job_context->barrier;
  freeShuffleVector(job_context);
  freeThreadVector (job_context);
  delete job_context->threads_contexts;
  free (job_context);
  job = nullptr;
}

