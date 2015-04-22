# SpotManager

Find cheapest spot instance prices, bid, use, and teardown when no longer needed

## Assumptions

The module assumes your workload is **long running** and has **many save-points**.    

In my case each machine is setup to pull small tasks off a queue and execute them.  These machines can be shutdown at any time; with the most recent task simply placed back on the queue for some other machine to run.   

## Overview

This library works on a concept of ***utility***, which is an abstract value you assign to each EC2 instance type; the ***required utility*** is the primary input used to scale the number and type of instances. 

For each instance type, the `SpotManager` finds the 80% percentile price point[1] from the past 24 hours, combines it with a `utility` score for that instance type to get an `estimated_value` (measured in utility per dollar).  The instance type with the best `estimated_value` is identified and bid on.

## Requirements

* Python 2.7
* boto
* requests
* ecdsa (required by fabric, but not installed by pip)
* fabric

## Installation

For now, you must clone the repo

	git clone https://github.com/klahnakoski/SpotManager.git

## Configuration

Each SpotManager instance requires a `settings.json` file that controls the SpotManager behaviour.  We will use the [ActiveData ETL settings file](resources/config/etl_settings.json) as an example to explain the parameters

	
* **`budget`** - Acts as an absolute spend limit for this instance.  Be sure you know your limits.
* **`max_utility_price`** - Whatever you decide a unit of ***utility*** is, you should set the highest price you are willing to pay for one.  This can ensure you do not go over the on-demand price, and prevents the SpotManager from bidding when everything is too expensive.
* **`price_file`** - To minimize AWS calls, the previous price data is stored in a file for retrieval next time.
* **`aws`** - a structure containing the parameters to [connect to AWS using boto](http://boto.readthedocs.org/en/latest/ref/ec2.html#boto.ec2.connection.EC2Connection)
* **`utility`** - a list of objects declaring the utility of each instance type.  Instance types not mentioned are assumed to have zero utility and will not be bid on, but will be terminated if any exist. 
* **`ec2.request`** - template for making a [spot request using boto](http://boto.readthedocs.org/en/latest/ref/ec2.html#boto.ec2.connection.EC2Connection.request_spot_instances). This is where you declare the machine image, private keys, networking interfaces, etc.
* **`ec2.instance.name`** - Name that will be assigned to an instance (and to the spot requests).  It is important that no other machines under the AWS user have this prefix.  ***Any machines with this prefix will be under the control of SpotManager.***    
* **`instance`** -  The parameters that will be sent to the constructor for your `InstanceManager`. 
* **`instance.class`** - An additional property in `instance`: The full name of the class you are using to setup/teardown an instance.
* **`debug`** - Settings for the [logging module](https://github.com/klahnakoski/SpotManager/blob/master/pyLibrary/debugs/README.md#configuration)

### Writing a InstanceManager

Conceptually, an instance manager is very simple, with only three methods you need to implement.  This repo has an [example in `active_data.etl`](https://github.com/klahnakoski/SpotManager/blob/master/active_data/etl.py) that you can review. 

* **`required_utility()`** - function to determine how much utility is needed.  Since you are the one defining utility, the amount you need is also up to you.  The `active_data` uses the size of the pending queue to determine, roughly, how much utility is required.
* **`setup_instance()`** - function is called to setup an instance.  It is passed both a boto ec2 instance object, and the utility this instance is expected to provide. 
* **`teardown_instance()`** - When the machine is no longer required, this will be called before the machine is terminated.  


## Benefits

The benefit of an 80% price point is we want a reasonable up-time with a low price.  We do not want a price set too high: we desire Amazon-initiated termination so we get the last partial hour free.  Also, some of instance types have unpredictable and extreme price swings; `SpotManager` allows you to utilize those valleys at minimal price exposure.

The more instance types your workload can run on, the more advantage you have finding minimal pricing:  Anecdotally, there is always an opportunity to be found: There is always an instance type going for significantly less than its utility would indicate.


[1] 80% percentile price point - the price that is over the current price 80% of the time

