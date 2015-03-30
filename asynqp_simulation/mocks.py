import asynqp
import asyncio
from collections import deque


class MockConnection:
    
    @asyncio.coroutine
    def open_channel(self, loop):
        return MockChannel(asyncio.get_event_loop() if loop is None else loop)


    @asyncio.coroutine
    def close(self):
        pass


class MockChannel:
    
    def __init__(self, loop):
        self.qeueus = {}
        self.exchanges = {}
        self.loop = loop
        self.messages = []
        self.undeliverableMessages = 0
        self.returnHandler = None
        
    
    def countUndeliveredMessages(self):
        return self.undeliverableMessages
    
    
    def countDeliveredMessages(self):
        return len(self.messages)
    
    
    def countAckedMessages(self):
        return len([True for message in self.messages if message.acked])
    
    
    def countRejectedMessages(self):
        return len([True for message in self.messages if message.rejected])
    
        
    @asyncio.coroutine
    def declare_queue(self, name='', *, durable=True, exclusive=False, auto_delete=False):
        if name in self.qeueus:
            return self.qeueus[name]
        else:
            self.qeueus[name] = MockQueue(name, self)
            return self.qeueus[name]

    
    @asyncio.coroutine
    def declare_exchange(self, name, type, *, durable=True, auto_delete=False, internal=False):
        if name in self.exchanges:
            return self.exchanges[name]
        else:
            if name == '':
                exchange = MockExchange(name, 'direct', self)
            else:
                exchange = MockExchange(name, type, self)
                
            self.exchanges[name] = exchange
            return exchange
    
    
    @asyncio.coroutine
    def set_qos(self, prefetch_size=0, prefetch_count=0, apply_globally=False):
        pass
    
    
    def set_return_handler(self, handler):
        self.returnHandler = handler
    

class MockQueue:
    
    def __init__(self, name, channel):
        self.name = name
        self.callbacks = []
        self.loop = channel.loop
        self.channel = channel
        self.awaitingMessages = deque()
        
        
    @asyncio.coroutine
    def bind(self, exchange, routingKey):
        exchange.bindQueue(routingKey, self)
        return None # TODO: implement MockQueueBinding with unbind method


    @asyncio.coroutine
    def consume(self, callback, *, no_local=False, no_ack=False, exclusive=False):
        self.callbacks.append(callback)
        return None # TODO: implement MockConsumer with cancel method


    # internal method for mocking
    def deliverMessage(self, message):
        if len(self.awaitingMessages) > 0:
            awaitingMessage = self.awaitingMessages.popleft()
            awaitingMessage.set_result(message)
        elif len(self.callbacks) > 0:
            self.callbacks[0](message)
        elif self.channel.returnHandler is not None:
            self.channel.returnHandler(message)
        else:
            raise ValueError("There was no consumer for the message with routing key " + message.routing_key + "!")


    @asyncio.coroutine
    def get(self, *, no_ack=False):
        awaitingMessage = asyncio.Future()
        self.awaitingMessages.append(awaitingMessage)
        message = yield from awaitingMessage
        return message


    @asyncio.coroutine
    def purge(self):
        pass


    @asyncio.coroutine
    def delete(self, *, if_unused=True, if_empty=True):
        pass
   

class MockMessage(asynqp.Message):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.acked = False
        self.rejected = False


class MockIncomingMessage(asynqp.Message):
    
    def __init__(self, message, routingKey, exchange_name):
        self.body = message.body
        self.properties = message.properties
        self.message = message
        self.routing_key = routingKey
        self.exchange_name = exchange_name
        
        
    def ack(self):
        self.message.acked = True


    def reject(self, *, requeue=True):
        self.message.rejected = True


class MockExchange:
    
    def __init__(self, name, type, channel):
        self.name = name
        self.type = type
        self.channel = channel
        self.bindings = []
        
    
    # internal method for mocking
    def bindQueue(self, routingKey, queue):
        self.bindings.append((routingKey, queue))
        
    
    def publish(self, message, routingKeyForMessage):
        matchingQueues = []
        
        if self.type == 'direct':
            matchingQueues = [queue for routingKey, queue in self.bindings if routingKey == routingKeyForMessage]
        elif self.type == 'fanout':
            matchingQueues = [queue for routingKey, queue in self.bindings]
        elif self.type == 'topic':
            matchingQueues = [queue for pattern, queue in self.bindings if matchingRoutingKey(routingKeyForMessage, pattern)]
        elif self.type == 'headers':
            raise ValueError("Headers routing is not implemented at the moment!")
        else:
            raise ValueError("Invalid exchange type!")

        if len(matchingQueues) > 0:
            for queue in matchingQueues:
                incomingMessage = MockIncomingMessage(message, routingKeyForMessage, self.name)
                self.channel.messages.append(incomingMessage)
                asyncio.get_event_loop().call_soon(queue.deliverMessage, incomingMessage)
        else:
            self.channel.undeliverableMessages = self.channel.undeliverableMessages + 1

        

def matchingRoutingKey(routingKey, pattern):
    if pattern == '#':
        return True
    
    routingElements = routingKey.split('.')
    patternElements = pattern.split('.')
    
    if len(routingElements) == 0:
        return False
    
    # compare each pattern element with the corresponding routing element
    for pos, patternElement in enumerate(patternElements):
        if patternElement == '#':
            # We allow the hash at the end, other positions are not supported at the moment
            if pos == len(patternElements) - 1:
                return True
            else:
                raise ValueError('Pattern matching with "#" hashes within the pattern is not implemented yet!')
            
        if not (patternElement == '*' or patternElement == routingElements[pos]):
            return False
        
    # if the routing key is longer and the pattern doesnt end with # (was checked before)
    if len(routingElements) > len(patternElements):
        return False
        
    return True
        
            