# SpotManager

Find cheapest spot instance prices, bid, use, and teardown when done

## Assumptions

The module assumes your workload is **long running** and has **many save-points**.    

In my case each machine is setup to pull small tasks off a queue and execute them.  These machines can be shutdown at any time; with the most recent task simply placed back on the queue for some other machine to run.   

# Features

For each instance type, the `SpotManager` finds the 80% percentile price point[1] from the past 24 hours, combines it with a `utility` score for that instance type to get an `estimated_value` (measured in utility per dollar).  The instance type with the best `estimated_value` is identified and bid on.

# Benefits

The benefit of an 80% price point is we want a reasonable up-time with a low price.  We do not want a price set too high: we desire Amazon-initiated termination so we get the last partial hour free.  Also, some of instance types have unpredictable and extreme price swings; `SpotManager` allows you to utilize those valleys at minimal price exposure.

The more instance types your workload can run on, the more advantage you have finding minimal pricing:  Anecdotally, there is always an opportunity to be found: There is always an instance type going for significantly less than its utility would indicate.

## Overview

This library works on a concept of **utility**, which is an abstract value you assign to each EC2 instance type; the **required utility** is the primary input used to scale the number and type of instances. 


[1] 80% percentile price point - over a given duration, find the 80% with the lowest prices, pick the maximum of those.

## Setup

### Assign utility to each instance type

### Determine current required utility
