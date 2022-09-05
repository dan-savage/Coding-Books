# Queue

## Package Layout
- Split the package into the implementation, and the queue types we use 

### Queue.go

- Thirdpart queue systems call the Register method. 
- This is done in the init function of the thirdparty queue files, which registers by the thirdparty scheme 
- So we've got a global queue registery, which is a map, which lets us join queues onto it
- A MustQueue funciton calls the Queue function, which 
    1) Parses the queue uri into a url, figures out which type it is, and then looks up that queue in the registry
    2) The registry then parses back a "mux object" of that type , 
    3) so sqs://test would then have scheme sqs, which would then be looked up in the global mux registery, and return an sqs mux
    4) We then return the mux.Queue method call of that given mux, as we parse the queue uri, and this returns the Queue handler and error
    5) This creates a new queue handler, which is a struct with an in channel, an outgoing channel, a done channel, and a ready channel, plus the uri
    6) we then beign polling for incoming and outgoing messages, visibility, and then return this handler 

#### Polling 
    1) This is different per implementation of the queue uri given (and / or mux)
    2) Parse the URI, and use the thirdparty's api for accessing the queue 
    3) Establish the relevant consumer/producer connection 
    4) Create a goroutine which has a forever loop 
    5) In this forever loop (this is triggered by pushing a bool when in the Start() function), wait for the handler to be ready, then build a select switch
    6) In the select switch have a case for when the handler is done
    7) have a case which uses the third party api to receive a message, and when received, push this into the handler's receive channel

#### Start
    1) Once we start the Queue, we pass a bool into the Ready channel of the Queue Handler 
    2) Then start a goroutine which reads from the Queue handlers in channel, and loops through the queue handlers, trying to handle the message 
    3) Good place to have metrics here, to see the time take to process the message

Whne we add a handler to the queue, we call the q.AddHandler method, which esentially unmarshals the incoming message into the given struct of that specific handler
We then call the function passed to the Add***MessageHandler function 