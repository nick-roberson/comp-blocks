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
% poetry run python example.py
Starting the computation sequence.
Running computation on data:
   ColumnA  ColumnB
0        0       10
1        1       11
2        2       12
3        3       13
4        4       14
5        5       15
6        6       16
7        7       17
8        8       18
9        9       19
2024-04-23 20:18:07 - INFO - Starting SequentialRunner at 2024-04-23 20:18:07.818705 with input shape (10, 2)
2024-04-23 20:18:07 - INFO - Starting SequentialRunner at 2024-04-23 20:18:07.818779 with input shape (10, 2)
2024-04-23 20:18:07 - INFO - Starting PrepareBlock at 2024-04-23 20:18:07.818813 with input shape (10, 2)
2024-04-23 20:18:07 - INFO - Finished PrepareBlock at 2024-04-23 20:18:07.820001 in 0:00:00.001188 with output shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting ParallelRunner at 2024-04-23 20:18:07.820039 with input shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting ParallelRunner at 2024-04-23 20:18:07.820057 with input shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting AddNBlock at 2024-04-23 20:18:07.820239 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished AddNBlock at 2024-04-23 20:18:07.820442 in 0:00:00.000203 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting AddNBlock at 2024-04-23 20:18:07.820535 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished AddNBlock at 2024-04-23 20:18:07.820811 in 0:00:00.000276 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting AddNBlock at 2024-04-23 20:18:07.820956 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished AddNBlock at 2024-04-23 20:18:07.821161 in 0:00:00.000205 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting AddNBlock at 2024-04-23 20:18:07.821193 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished AddNBlock at 2024-04-23 20:18:07.821337 in 0:00:00.000144 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting AddNBlock at 2024-04-23 20:18:07.821363 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished AddNBlock at 2024-04-23 20:18:07.821556 in 0:00:00.000193 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished ParallelRunner at 2024-04-23 20:18:07.821888 in 0:00:00.001831 with output shape (10, 3)
2024-04-23 20:18:07 - INFO - Finished ParallelRunner at 2024-04-23 20:18:07.821920 in 0:00:00.001881 with output shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting ParallelRunner at 2024-04-23 20:18:07.821941 with input shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting ParallelRunner at 2024-04-23 20:18:07.821955 with input shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting MultiplyByNBlock at 2024-04-23 20:18:07.822077 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished MultiplyByNBlock at 2024-04-23 20:18:07.822379 in 0:00:00.000302 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting MultiplyByNBlock at 2024-04-23 20:18:07.822422 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting MultiplyByNBlock at 2024-04-23 20:18:07.822531 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting MultiplyByNBlock at 2024-04-23 20:18:07.822577 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished MultiplyByNBlock at 2024-04-23 20:18:07.822983 in 0:00:00.000561 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished MultiplyByNBlock at 2024-04-23 20:18:07.823266 in 0:00:00.000735 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Starting MultiplyByNBlock at 2024-04-23 20:18:07.823299 with input shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished MultiplyByNBlock at 2024-04-23 20:18:07.823486 in 0:00:00.000909 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished MultiplyByNBlock at 2024-04-23 20:18:07.823745 in 0:00:00.000446 with output shape (2, 3)
2024-04-23 20:18:07 - INFO - Finished ParallelRunner at 2024-04-23 20:18:07.824181 in 0:00:00.002226 with output shape (10, 3)
2024-04-23 20:18:07 - INFO - Finished ParallelRunner at 2024-04-23 20:18:07.824216 in 0:00:00.002275 with output shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting SequentialRunner at 2024-04-23 20:18:07.824238 with input shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting SequentialRunner at 2024-04-23 20:18:07.824255 with input shape (10, 3)
2024-04-23 20:18:07 - INFO - Starting AverageBlock at 2024-04-23 20:18:07.824272 with input shape (10, 3)
2024-04-23 20:18:07 - INFO - Finished AverageBlock at 2024-04-23 20:18:07.824560 in 0:00:00.000288 with output shape (10, 4)
2024-04-23 20:18:07 - INFO - Finished SequentialRunner at 2024-04-23 20:18:07.824586 in 0:00:00.000331 with output shape (10, 4)
2024-04-23 20:18:07 - INFO - Finished SequentialRunner at 2024-04-23 20:18:07.824599 in 0:00:00.000361 with output shape (10, 4)
2024-04-23 20:18:07 - INFO - Finished SequentialRunner at 2024-04-23 20:18:07.824618 in 0:00:00.005839 with output shape (10, 4)
2024-04-23 20:18:07 - INFO - Finished SequentialRunner at 2024-04-23 20:18:07.824629 in 0:00:00.005924 with output shape (10, 4)

Completed all computations in 0.005938 seconds.
Cols: ['id', 'column_a', 'column_b', 'column_a_avg']
Head:
                 id  column_a  column_b  column_a_avg
0  73d9d92cd0c229d2         0        30           4.5
1  9bc4cd485fe277f9         1        32           4.5
2  d362ce1155cb0aed         2        34           4.5
3  6f322ab92247df40         3        36           4.5
4  8fb0ab3bf31047c1         4        38           4.5
5  89512ae1404ffa1a         5        40           4.5
6  fb88400d2b8ff1c8         6        42           4.5
7  c1cf06a78a87c1ab         7        44           4.5
8  554e301e92851d82         8        46           4.5
9  bc24c3a5ce2dbf73         9        48           4.5
```

This will run the example.py file, which demonstrates how to use the framework and comes with a few simple and more complex examples.

To view the code, just open the example.py file in a text editor.