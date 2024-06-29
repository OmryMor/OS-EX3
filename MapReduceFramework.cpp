//
// Created by iritv on 6/29/2024.
//
#include "MapReduceFramework.h"
#include <csignal>
#include <vector>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <bits/semaphore.h>
#include <semaphore.h>

/*************************************************************************
 *                           Defines                                     *
 *************************************************************************/
#define SYSTEM_ERROR_MSG "system error: "
#define SUCCESS_RET_VAL 0

/*************************************************************************
 *                           TypeDefs                                    *
 *************************************************************************/

typedef struct
{
    MapReduceClient &client;
    InputVec inputVec;
    OutputVec &outputVec;
    int multiThreadLevel;
    std::vector<IntermediateVec> intermediateVecs;
    std::atomic<int> atomicCounter;
    std::vector<pthread_t> threads;
    JobState state;
    pthread_mutex_t mutex;
    sem_t semaphore;

} JobContext;

JobContext *createJobContext (const MapReduceClient &client,
                              const InputVec &inputVec, OutputVec &outputVec,
                              int multiThreadLevel)
{
  auto *job_context = (JobContext *) (malloc (sizeof (JobContext)));
  if (job_context == nullptr)
  {
    std::cout << SYSTEM_ERROR_MSG << "Failed to allocate memory." << std::endl;
    //TODO free memory?
    exit (1);
  }

  job_context->client=client;
  job_context->inputVec = inputVec;
  job_context->outputVec = outputVec;
  job_context->intermediateVecs.resize (multiThreadLevel);
  job_context->multiThreadLevel = multiThreadLevel;
  job_context->atomicCounter = 0;
  job_context->threads.resize (multiThreadLevel);
  job_context->state = {UNDEFINED_STAGE, 0.0f};
  job_context->mutex = PTHREAD_MUTEX_INITIALIZER;
  sem_init(&job_context->semaphore, 0, 0);

  return job_context;
}

void
createWorkingThreads (const std::vector<pthread_t> &threads, int numberOfThreads)
{
  for (int i = 0; i < numberOfThreads; ++i)
  {
    int ret_val = pthread_create (&threads[i], nullptr, threadFunc, nullptr);
    if (ret_val != SUCCESS_RET_VAL)
    {
      std::cout << SYSTEM_ERROR_MSG << "Failed creating thread" << std::endl;
      //TODO free memory?
      exit (1);
    }
  }
}

// Function to get JobContext from JobHandle
JobContext *getJobContext (JobHandle jobHandle)
{
  return static_cast<JobContext *>(jobHandle);
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  //TODO check valid arguments
  JobContext *job_context = createJobContext (client,inputVec,outputVec,multiThreadLevel);
  job_context->state.stage = MAP_STAGE;

  // run map function
  for(int i=0; i<multiThreadLevel; i++){
    pthread_create (job_context->threads[i], nullptr,client.map, nullptr);
  }
  return static_cast<JobHandle>(job_context);
}

void waitForJob (JobHandle job)
{
  //TODO add mutex?
  JobContext *job_context = getJobContext (job);
  for (pthread_t thread: job_context->threads)
  {
    int ret_val = pthread_join (thread, nullptr);
    if (ret_val != SUCCESS_RET_VAL)
    {
      std::cout << SYSTEM_ERROR_MSG << "Failed calling pthread_join" <<
                std::endl;
      //TODO free memory?
      exit (1);
    }
  }
}

void getJobState (JobHandle job, JobState *state)
{
  JobContext *job_context = getJobContext (job);
  job_context->state = {state->stage, state->percentage};
}

void closeJobHandle (JobHandle job)
{
  //TODO add mutex?
  waitForJob (job);
  JobContext *job_context = getJobContext (job);
  pthread_mutex_destroy (&job_context->mutex);
  //TODO destroy semaphores
  free (job_context);
  job = nullptr;
}