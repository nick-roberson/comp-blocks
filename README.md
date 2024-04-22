# Comp Blocks

## Description

Framework for organizing and running computation blocks on an input dataframe. 
The two main components here are the SequentialRunner and ParallelRunner classes.
These two classes perform the same function, but the ParallelRunner class is able to run the computation blocks in parallel, while the SequentialRunner class runs the computation blocks in sequence.

## Installation

Poetry is used to manage dependencies. To install the dependencies, run the following commands:
```bash
% poetry install 
% poetry update
```

## Usage

To view an example of this framework in action, run the following command:
```bash
% poetry run python example
```
This will run the example.py file, which demonstrates how to use the framework and comes with a few simple and more complex examples.

To view the code, just open the example.py file in a text editor.