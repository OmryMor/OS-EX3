//
// Created by iritv on 6/29/2024.
//
#include "MapReduceFramework.h"
#include <csignal>
#include <vector>
#include <atomic>
#include <cstdlib>
#include <iostream>

/*************************************************************************
 *                           Defines                                     *
 *************************************************************************/
#define SYSTEM_ERROR_MSG "system error: "

/*************************************************************************
 *                           TypeDefs                                    *
 *************************************************************************/

typedef struct
{
    std::vector<pthread_t> threads;
    JobState state;
    pthread_mutex_t mutex;
    std::atomic<int> processedKeys;
    std::atomic<int> totalKeys;
} JobContext;

JobContext* createJobContext (int numberOfThreads)
{
  auto *job_context = (JobContext *) (malloc (sizeof (JobContext)));
  if (job_context == nullptr)
  {
    std::cout << SYSTEM_ERROR_MSG << "Failed to allocate memory." << std::endl;
    //TODO free memory?
    exit (1);
  }
  job_context->threads.resize (numberOfThreads);
  job_context->state = {UNDEFINED_STAGE, 0.0f};
  job_context->processedKeys = 0;
  job_context->totalKeys = 0;
  int ret_val = pthread_mutex_init (&job_context->mutex, nullptr);
  if (ret_val != 0)
  {
    std::cout << SYSTEM_ERROR_MSG << "Failed creating mutex" << std::endl;
    //TODO free memory?
    exit (1);
  }
  return job_context;
}

void
createWorkingThreads (const std::vector<pthread_t> &threads, int numberOfThreads)
{
  for (int i = 0; i < numberOfThreads; ++i)
  {
    int ret_val = pthread_create (&threads[i], nullptr, threadFunc, nullptr);
    if (ret_val != 0)
    {
      std::cout << SYSTEM_ERROR_MSG << "Failed creating thread" << std::endl;
      //TODO free memory?
      exit (1);
    }
  }
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  //TODO check valid arguments
  JobContext *job_context = createJobContext (multiThreadLevel);
  createWorkingThreads (job_context->threads, multiThreadLevel);


  return static_cast<JobHandle>(job_context);
}