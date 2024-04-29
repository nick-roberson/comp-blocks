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

To view an example of this framework in action, run the following command (there is also a more detailed example in the taxi_model_example.py file, but requires a larger dataset and more complex env setup):
```bash
% poetry run python simple_example.py
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

# Block Processing Starts using SequentialRunner
2024-04-23 20:32:49 - INFO - [1322320f-4add-4be2-9ce8-eed048ee7fe9] Starting SequentialRunner at 2024-04-23 20:32:49.771941 with input shape (10, 2)

# Example Run Block Alone
2024-04-23 20:32:49 - INFO - [420bfd44-93dc-4adb-a2f4-b700b2f9df3f] Starting PrepareBlock at 2024-04-23 20:32:49.772022 with input shape (10, 2)
2024-04-23 20:32:49 - INFO - [420bfd44-93dc-4adb-a2f4-b700b2f9df3f] Finished PrepareBlock at 2024-04-23 20:32:49.773199 in 0:00:00.001177 with output shape (10, 3)

# Example Run Block using ParallelRunner
2024-04-23 20:32:49 - INFO - [ed65e41f-77cd-4bed-9a63-5b02d8df9b7d] Starting ParallelRunner at 2024-04-23 20:32:49.773239 with input shape (10, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Starting AddNBlock at 2024-04-23 20:32:49.773411 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Finished AddNBlock at 2024-04-23 20:32:49.773669 in 0:00:00.000258 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Starting AddNBlock at 2024-04-23 20:32:49.773706 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Finished AddNBlock at 2024-04-23 20:32:49.773931 in 0:00:00.000225 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Starting AddNBlock at 2024-04-23 20:32:49.773965 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Starting AddNBlock at 2024-04-23 20:32:49.773984 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Starting AddNBlock at 2024-04-23 20:32:49.774113 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Finished AddNBlock at 2024-04-23 20:32:49.774311 in 0:00:00.000346 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Finished AddNBlock at 2024-04-23 20:32:49.774465 in 0:00:00.000481 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [84a2ba57-b786-4a5a-8483-27176c17246f] Finished AddNBlock at 2024-04-23 20:32:49.774642 in 0:00:00.000529 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [ed65e41f-77cd-4bed-9a63-5b02d8df9b7d] Finished ParallelRunner at 2024-04-23 20:32:49.775015 in 0:00:00.001776 with output shape (10, 3)

# Example Run Block using ParallelRunner
2024-04-23 20:32:49 - INFO - [2609a3c2-6065-4147-955b-f9fe654bc04b] Starting ParallelRunner at 2024-04-23 20:32:49.775046 with input shape (10, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Starting MultiplyByNBlock at 2024-04-23 20:32:49.775159 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Starting MultiplyByNBlock at 2024-04-23 20:32:49.775274 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Finished MultiplyByNBlock at 2024-04-23 20:32:49.775420 in 0:00:00.000261 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Finished MultiplyByNBlock at 2024-04-23 20:32:49.775692 in 0:00:00.000418 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Starting MultiplyByNBlock at 2024-04-23 20:32:49.775731 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Starting MultiplyByNBlock at 2024-04-23 20:32:49.775795 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Starting MultiplyByNBlock at 2024-04-23 20:32:49.775815 with input shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Finished MultiplyByNBlock at 2024-04-23 20:32:49.776006 in 0:00:00.000275 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Finished MultiplyByNBlock at 2024-04-23 20:32:49.776196 in 0:00:00.000401 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [555adc6f-6192-4382-b14d-7ddcece803b5] Finished MultiplyByNBlock at 2024-04-23 20:32:49.776385 in 0:00:00.000570 with output shape (2, 3)
2024-04-23 20:32:49 - INFO - [2609a3c2-6065-4147-955b-f9fe654bc04b] Finished ParallelRunner at 2024-04-23 20:32:49.776684 in 0:00:00.001638 with output shape (10, 3)

# Example Run Block using SequentialRunner
2024-04-23 20:32:49 - INFO - [d49b897c-2f9a-4238-bd98-1d0c4d80cede] Starting SequentialRunner at 2024-04-23 20:32:49.776713 with input shape (10, 3)
2024-04-23 20:32:49 - INFO - [eb103c9a-7aec-42d0-9a16-a19a5ae4278f] Starting AverageBlock at 2024-04-23 20:32:49.776731 with input shape (10, 3)
2024-04-23 20:32:49 - INFO - [eb103c9a-7aec-42d0-9a16-a19a5ae4278f] Finished AverageBlock at 2024-04-23 20:32:49.776985 in 0:00:00.000254 with output shape (10, 4)
2024-04-23 20:32:49 - INFO - [d49b897c-2f9a-4238-bd98-1d0c4d80cede] Finished SequentialRunner at 2024-04-23 20:32:49.777020 in 0:00:00.000307 with output shape (10, 4)

2024-04-23 20:32:49 - INFO - [1322320f-4add-4be2-9ce8-eed048ee7fe9] Finished SequentialRunner at 2024-04-23 20:32:49.777044 in 0:00:00.005103 with output shape (10, 4)

Completed all computations in 0.005119 seconds.
Cols: ['id', 'column_a', 'column_b', 'column_a_avg']
Head:
                 id  column_a  column_b  column_a_avg
0  73d9d92cd0c229d2        10        10          19.0
1  9bc4cd485fe277f9        12        11          19.0
2  d362ce1155cb0aed        14        12          19.0
3  6f322ab92247df40        16        13          19.0
4  8fb0ab3bf31047c1        18        14          19.0
5  89512ae1404ffa1a        20        15          19.0
6  fb88400d2b8ff1c8        22        16          19.0
7  c1cf06a78a87c1ab        24        17          19.0
8  554e301e92851d82        26        18          19.0
9  bc24c3a5ce2dbf73        28        19          19.0

```

This will run the example.py file, which demonstrates how to use the framework and comes with a few simple and more complex examples.

To view the code, just open the example.py file in a text editor.