# TaskForge - Self-Hosted Distributed Task Queue Service

### Project Overview

The goal is to build TaskForge — a self-hosted distributed task queue with HTTP enqueue API, priority lanes, configurable retry policies, dead-letter queues, worker heartbeats, and a real-time admin dashboard. Think of it as a stripped-down Google Cloud Tasks you fully understand because you built it.

### Core Concepts

A queue holds tasks of a similar type. A task is a unit of work with a payload, delivery schedule, and retry budget. Workers pull tasks and execute them — they report back success, failure, or ask for a retry. The scheduler handles delayed tasks and retry backoff. The dispatcher is the engine that moves tasks from `pending` to `claimed` to workers.
