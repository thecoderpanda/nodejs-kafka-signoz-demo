const express = require('express');
const { Kafka } = require('kafkajs');
const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {

  await producer.connect();

  // Consuming messages
  await consumer.connect();
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
    },
  });
};

run().catch(console.error);

// Endpoint to produce a message
app.post('/produce', async (req, res) => {
  const { topic, message } = req.body;
  try {
    await producer.send({
      topic,
      messages: [
        { value: message },
      ],
    });
    res.status(200).json({ message: 'Message produced successfully' });
  } catch (error) {
    console.error("Error producing message", error);
    res.status(500).json({ message: 'Error producing message' });
  }
});

let messages = [];

consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    messages.push(message.value.toString());
  },
});

app.get('/consume', (req, res) => {
  res.status(200).json({ messages });
  messages = []; 
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
