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

### Simple Example
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

This will run the `simple_example.py` file, which demonstrates how to use the framework and comes with a few simple and more complex examples.

### Model Training Example

In this example I will show how you can use this framework to train a simple model using the `taxi_model_example.py` file. 
This will use the NYC Taxi dataset to train a simple model that predicts the fare amount based on the trip distance as well
as other features.

```bash
% python taxi_model_example.py 

Loaded data with 120000 records.

# Block Processing Starts using SequentialRunner
2024-04-30 14:45:33 - INFO - [5774691e-ed67-44df-9c2c-dfb316b5b3ba] Starting SequentialRunner at 2024-04-30 14:45:33.109121 with input shape (120000, 8)

# Pre-processing the data for training purposes
2024-04-30 14:45:33 - INFO - [455c2e57-a24e-4c7e-b5de-dfb015e5823d] Starting ParallelRunner at 2024-04-30 14:45:33.109231 with input shape (120000, 8)

2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.109760 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.109842 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.110939 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.111834 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.131304 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.171340 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.193932 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.236888 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.353005 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.424272 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.594571 with input shape (10000, 8)
2024-04-30 14:45:33 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Starting PrepareTaxiBlock at 2024-04-30 14:45:33.663233 with input shape (10000, 8)
2024-04-30 14:45:34 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:34.848031 in 0:00:01.738189 with output shape (10000, 15)
2024-04-30 14:45:34 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:34.902148 in 0:00:01.708216 with output shape (10000, 15)
2024-04-30 14:45:34 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:34.903448 in 0:00:01.793688 with output shape (10000, 15)
2024-04-30 14:45:34 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:34.911598 in 0:00:01.800659 with output shape (10000, 15)
2024-04-30 14:45:34 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:34.938276 in 0:00:01.826442 with output shape (10000, 15)
2024-04-30 14:45:35 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:35.008524 in 0:00:01.655519 with output shape (10000, 15)
2024-04-30 14:45:35 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:35.058661 in 0:00:01.821773 with output shape (10000, 15)
2024-04-30 14:45:35 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:35.060530 in 0:00:01.636258 with output shape (10000, 15)
2024-04-30 14:45:35 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:35.061244 in 0:00:01.466673 with output shape (10000, 15)
2024-04-30 14:45:35 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:35.061892 in 0:00:01.890552 with output shape (10000, 15)
2024-04-30 14:45:35 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:35.063484 in 0:00:01.932180 with output shape (10000, 15)
2024-04-30 14:45:35 - INFO - [c0631ce9-991d-44b1-8352-6dbc63a81797] Finished PrepareTaxiBlock at 2024-04-30 14:45:35.064000 in 0:00:01.400767 with output shape (10000, 15)

2024-04-30 14:45:35 - INFO - [455c2e57-a24e-4c7e-b5de-dfb015e5823d] Finished ParallelRunner at 2024-04-30 14:45:35.071418 in 0:00:01.962187 with output shape (120000, 15)

# Training the model
2024-04-30 14:45:35 - INFO - [47904289-ecfe-4ada-8697-03bc1552e855] Starting TrainModelBlock at 2024-04-30 14:45:35.071536 with input shape (120000, 15)
2024-04-30 14:45:35 - INFO - ******************************************** VALIDATE
2024-04-30 14:45:35 - INFO - ******************************************** PREPARE TENSORS AND SETUP MODEL
2024-04-30 14:45:35 - INFO - ******************************************** TRAIN THE MODEL
2024-04-30 14:45:35 - INFO - Epoch 0: Loss = 12.44249153
2024-04-30 14:45:38 - INFO - Epoch 25: Loss = 10.67524052
2024-04-30 14:45:42 - INFO - Epoch 50: Loss = 10.04615688
2024-04-30 14:45:45 - INFO - Epoch 75: Loss = 9.62959385
2024-04-30 14:45:48 - INFO - Epoch 100: Loss = 9.08148861
2024-04-30 14:45:51 - INFO - Epoch 125: Loss = 8.29010868
2024-04-30 14:45:54 - INFO - Epoch 150: Loss = 7.25615215
2024-04-30 14:45:57 - INFO - Epoch 175: Loss = 6.08520365
2024-04-30 14:46:01 - INFO - Epoch 200: Loss = 4.87070942
2024-04-30 14:46:04 - INFO - Epoch 225: Loss = 4.09356451
2024-04-30 14:46:07 - INFO - Epoch 250: Loss = 3.82572341
2024-04-30 14:46:11 - INFO - Epoch 275: Loss = 3.70043707
2024-04-30 14:46:14 - INFO - Epoch 300: Loss = 3.64073944
2024-04-30 14:46:17 - INFO - Epoch 325: Loss = 3.60307598
2024-04-30 14:46:20 - INFO - Epoch 350: Loss = 3.54548311
2024-04-30 14:46:24 - INFO - Epoch 375: Loss = 3.49278474
2024-04-30 14:46:27 - INFO - Epoch 400: Loss = 3.47045970
2024-04-30 14:46:31 - INFO - Epoch 425: Loss = 3.43335009
2024-04-30 14:46:34 - INFO - Epoch 450: Loss = 3.40467238
2024-04-30 14:46:37 - INFO - Epoch 475: Loss = 3.37391090
2024-04-30 14:46:41 - INFO - ******************************************** EVALUATE THE MODEL
2024-04-30 14:46:41 - INFO - Final RMSE: 3.43234730
2024-04-30 14:46:41 - INFO - Predictions:
2024-04-30 14:46:41 - INFO -  1. Predicted: 7.2982, Actual: 7.3000, Diff: 0.0018
2024-04-30 14:46:41 - INFO -  2. Predicted: 6.0447, Actual: 6.1000, Diff: 0.0553
2024-04-30 14:46:41 - INFO -  3. Predicted: 5.3569, Actual: 5.3000, Diff: 0.0569
2024-04-30 14:46:41 - INFO -  4. Predicted: 7.4172, Actual: 7.3000, Diff: 0.1172
2024-04-30 14:46:41 - INFO -  5. Predicted: 7.5826, Actual: 7.7000, Diff: 0.1174
2024-04-30 14:46:41 - INFO -  6. Predicted: 6.7609, Actual: 6.9000, Diff: 0.1391
2024-04-30 14:46:41 - INFO -  7. Predicted: 11.9607, Actual: 12.1000, Diff: 0.1393
2024-04-30 14:46:41 - INFO -  8. Predicted: 6.6778, Actual: 6.5000, Diff: 0.1778
2024-04-30 14:46:41 - INFO -  9. Predicted: 5.1609, Actual: 4.9000, Diff: 0.2609
2024-04-30 14:46:41 - INFO - 10. Predicted: 10.6120, Actual: 10.9000, Diff: 0.2880
2024-04-30 14:46:41 - INFO - 11. Predicted: 15.6002, Actual: 15.3000, Diff: 0.3002
2024-04-30 14:46:41 - INFO - 12. Predicted: 11.6280, Actual: 12.1000, Diff: 0.4720
2024-04-30 14:46:41 - INFO - 13. Predicted: 6.0081, Actual: 6.5000, Diff: 0.4919
2024-04-30 14:46:41 - INFO - 14. Predicted: 7.0835, Actual: 6.5000, Diff: 0.5835
2024-04-30 14:46:41 - INFO - 15. Predicted: 6.7668, Actual: 6.1000, Diff: 0.6668
2024-04-30 14:46:41 - INFO - 16. Predicted: 3.9848, Actual: 3.3000, Diff: 0.6848
2024-04-30 14:46:41 - INFO - 17. Predicted: 8.4010, Actual: 7.7000, Diff: 0.7010
2024-04-30 14:46:41 - INFO - 18. Predicted: 4.4362, Actual: 3.7000, Diff: 0.7362
2024-04-30 14:46:41 - INFO - 19. Predicted: 15.8408, Actual: 14.9000, Diff: 0.9408
2024-04-30 14:46:41 - INFO - 20. Predicted: 8.3442, Actual: 7.3000, Diff: 1.0442
2024-04-30 14:46:41 - INFO - 21. Predicted: 5.8500, Actual: 6.9000, Diff: 1.0500
2024-04-30 14:46:41 - INFO - 22. Predicted: 8.3711, Actual: 7.3000, Diff: 1.0711
2024-04-30 14:46:41 - INFO - 23. Predicted: 17.6115, Actual: 16.5000, Diff: 1.1115
2024-04-30 14:46:41 - INFO - 24. Predicted: 5.6976, Actual: 6.9000, Diff: 1.2024
2024-04-30 14:46:41 - INFO - 25. Predicted: 9.4154, Actual: 8.1000, Diff: 1.3154
2024-04-30 14:46:41 - INFO - 26. Predicted: 5.1416, Actual: 6.5000, Diff: 1.3584
2024-04-30 14:46:41 - INFO - 27. Predicted: 5.0320, Actual: 6.5000, Diff: 1.4680
2024-04-30 14:46:41 - INFO - 28. Predicted: 11.1922, Actual: 9.7000, Diff: 1.4922
2024-04-30 14:46:41 - INFO - 29. Predicted: 11.2412, Actual: 9.7000, Diff: 1.5412
2024-04-30 14:46:41 - INFO - 30. Predicted: 10.9428, Actual: 12.5000, Diff: 1.5572
2024-04-30 14:46:41 - INFO - 31. Predicted: 10.1079, Actual: 8.5000, Diff: 1.6079
2024-04-30 14:46:41 - INFO - 32. Predicted: 6.1352, Actual: 4.5000, Diff: 1.6352
2024-04-30 14:46:41 - INFO - 33. Predicted: 8.0183, Actual: 9.7000, Diff: 1.6817
2024-04-30 14:46:41 - INFO - 34. Predicted: 6.6458, Actual: 4.9000, Diff: 1.7458
2024-04-30 14:46:41 - INFO - 35. Predicted: 9.5024, Actual: 7.7000, Diff: 1.8024
2024-04-30 14:46:41 - INFO - 36. Predicted: 11.8743, Actual: 14.1000, Diff: 2.2257
2024-04-30 14:46:41 - INFO - 37. Predicted: 21.7032, Actual: 19.3000, Diff: 2.4032
2024-04-30 14:46:41 - INFO - 38. Predicted: 8.5013, Actual: 5.7000, Diff: 2.8013
2024-04-30 14:46:41 - INFO - 39. Predicted: 7.6865, Actual: 10.5000, Diff: 2.8135
2024-04-30 14:46:41 - INFO - 40. Predicted: 46.5288, Actual: 49.5700, Diff: 3.0412
2024-04-30 14:46:41 - INFO - 41. Predicted: 7.1668, Actual: 4.1000, Diff: 3.0668
2024-04-30 14:46:41 - INFO - 42. Predicted: 4.7077, Actual: 8.1000, Diff: 3.3923
2024-04-30 14:46:41 - INFO - 43. Predicted: 9.2291, Actual: 5.7000, Diff: 3.5291
2024-04-30 14:46:41 - INFO - 44. Predicted: 7.4003, Actual: 3.7000, Diff: 3.7003
2024-04-30 14:46:41 - INFO - 45. Predicted: 14.7747, Actual: 10.5000, Diff: 4.2747
2024-04-30 14:46:41 - INFO - 46. Predicted: 8.8515, Actual: 13.7000, Diff: 4.8485
2024-04-30 14:46:41 - INFO - 47. Predicted: 11.1036, Actual: 16.1000, Diff: 4.9964
2024-04-30 14:46:41 - INFO - 48. Predicted: 10.4942, Actual: 16.1000, Diff: 5.6058
2024-04-30 14:46:41 - INFO - 49. Predicted: 4.8624, Actual: 13.3000, Diff: 8.4376
2024-04-30 14:46:41 - INFO - 50. Predicted: 10.1773, Actual: 37.1000, Diff: 26.9227
2024-04-30 14:46:41 - INFO - Model saved successfully to path '/Users/nicholas/Code/comp-blocks/TaxiFareRegrModel.pt'
2024-04-30 14:46:41 - INFO - [47904289-ecfe-4ada-8697-03bc1552e855] Finished TrainModelBlock at 2024-04-30 14:46:41.522408 in 0:01:06.450872 with output shape (120000, 15)

# Use the model to append predictions for all data and calculate the difference to the actual
2024-04-30 14:46:41 - INFO - [e3583e70-da88-4fec-a179-9db2bec9cc43] Starting ParallelRunner at 2024-04-30 14:46:41.522465 with input shape (120000, 15)

2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.522903 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.523012 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.523097 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.523466 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.526048 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.571558 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.573247 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.573431 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.574025 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.574399 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.575034 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Starting PredictBlock at 2024-04-30 14:46:41.575841 with input shape (10000, 15)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.674899 in 0:00:00.151996 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.675012 in 0:00:00.152000 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.686708 in 0:00:00.113461 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.692920 in 0:00:00.166872 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.698697 in 0:00:00.175231 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.698860 in 0:00:00.127302 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.700647 in 0:00:00.126622 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.700859 in 0:00:00.177762 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.701772 in 0:00:00.126738 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.701872 in 0:00:00.127473 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.702068 in 0:00:00.128637 with output shape (10000, 17)
2024-04-30 14:46:41 - INFO - [d77c09ac-4512-4c5d-b2e2-bfe37fb02b8c] Finished PredictBlock at 2024-04-30 14:46:41.702353 in 0:00:00.126512 with output shape (10000, 17)

2024-04-30 14:46:41 - INFO - [e3583e70-da88-4fec-a179-9db2bec9cc43] Finished ParallelRunner at 2024-04-30 14:46:41.709846 in 0:00:00.187381 with output shape (120000, 17)

# End of the outer sequential runner 
2024-04-30 14:46:41 - INFO - [5774691e-ed67-44df-9c2c-dfb316b5b3ba] Finished SequentialRunner at 2024-04-30 14:46:41.711056 in 0:01:08.601935 with output shape (120000, 17)

Dataframe Preview:
                 id          pickup_datetime  fare_class  pickup_longitude  pickup_latitude  dropoff_longitude  dropoff_latitude  ...  time_of_day am_or_pm weekday    dist_km fare_amount predictions  difference
0  5d2bac80127c04a5  2010-04-24 15:07:59 UTC           0        -73.953222        40.785891         -73.964796         40.764670  ...      morning       am     Sat   2.553010        7.30    7.478792   -0.178792
1  50b97c995afac155  2010-04-24 00:10:51 UTC           0        -73.981816        40.689890         -73.983086         40.722778  ...      evening       pm     Fri   3.658545        6.10   10.760161   -4.660161
2  17f20764b3f55206  2010-04-19 00:35:39 UTC           0        -73.960204        40.773240         -73.969933         40.758872  ...      evening       pm     Sun   1.795498        4.90    5.879290   -0.979290
3  b435198dd020eda2  2010-04-18 20:32:11 UTC           0        -73.988800        40.722179         -73.988213         40.729595  ...    afternoon       pm     Sun   0.826104        4.50    4.968929   -0.468929
4  454a93ea598ad8f3  2010-04-14 07:27:09 UTC           1        -74.000274        40.678055         -74.014794         40.706515  ...        night       am     Wed   3.393137       12.67   10.609567    2.060433
5  98f26407ff290031  2010-04-20 22:00:00 UTC           1        -73.971250        40.755682         -74.037350         40.614410  ...      evening       pm     Tue  16.668183       37.87   32.906429    4.963571
6  891eee02fba7f092  2010-04-13 17:21:03 UTC           0        -73.962758        40.758525         -73.982433         40.768337  ...    afternoon       pm     Tue   1.983972        7.30    8.572890   -1.272890
7  d70e4f45fab7b93d  2010-04-16 12:58:42 UTC           1        -73.942306        40.841825         -73.861603         40.768262  ...      morning       am     Fri  10.632439       29.07   28.260246    0.809754
8  2d258522df43574d  2010-04-12 19:23:45 UTC           0        -73.952757        40.803120         -73.984868         40.743790  ...    afternoon       pm     Mon   7.129835        3.70   15.187650  -11.487650
9  2ae4811e9ef70d0e  2010-04-21 11:29:10 UTC           1        -73.970482        40.758007         -73.968144         40.792296  ...      morning       am     Wed   3.817842       11.70   11.605367    0.094633

[10 rows x 17 columns]

Completed all work in 68.602006 seconds.

```