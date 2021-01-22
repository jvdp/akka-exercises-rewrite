initial-state

# Exercise 0 > Initial State

Before we begin coding, we want to verify that the initial state is working as
expected. From your `sbt` session: 

- Use the `run` command to check the main class `CoffeeHouseApp` boots up as 
expected. You should see the following:

```scala
man [e] > coffee-house > initial-state > run
[info] Running com.lightbend.training.coffeehouse.CoffeeHouseApp
[WARN] [05/27/2015 12:56:08.967] [run-main-0] [...coffeehouse.CoffeeHouseApp(akka://coffee-house-system)] CoffeeHouseApp running
Enter commands into the terminal, e.g. q or quit
```

- Use the `test` command to verify the initial state works as expected. You 
should see something like the following:

```scala
...
[info] Run completed in 527 milliseconds.
[info] Total number of tests run: 10
[info] Suites: completed 3, aborted 0
[info] Tests: succeeded 10, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 1 s, ...
```

- Use the `nextExercise` command to move to the next exercise.
