import amqplib from "amqplib";
import sharp from "sharp";
import { bucketName, s3Client } from "./s3.js";
import { GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

amqplib.connect("amqp://localhost", function () {});

const connection = await amqplib.connect("amqp://localhost");

const completedChannel = await connection.createChannel();
const toCompleteChannel = await connection.createChannel();

const completedImagesQueue = "completed-images";
const imagesToProcessQueue = "images-to-process";

completedChannel.assertQueue(completedImagesQueue, { durable: false });

toCompleteChannel.assertQueue(imagesToProcessQueue, { durable: false });

toCompleteChannel.consume(imagesToProcessQueue, async function (msg) {
  if (msg === null) return;

  const message = JSON.parse(msg.content.toString());

  const { Body } = await s3Client.send(
    new GetObjectCommand({
      Bucket: bucketName,
      Key: message.name,
    })
  );

  const file = await Body?.transformToByteArray();

  const rotatedImage = await sharp(file).rotate(180).toBuffer();

  const processedFileName = `processed-${message.name}`;

  await new Promise((resolve) => setTimeout(resolve, 5_000));

  await s3Client.send(
    new PutObjectCommand({
      Bucket: bucketName,
      Key: processedFileName,
      Body: rotatedImage,
    })
  );

  const successMessage = {
    processedName: processedFileName,
    name: message.name,
  };

  completedChannel.sendToQueue(
    completedImagesQueue,
    Buffer.from(JSON.stringify(successMessage))
  );

  toCompleteChannel.ack(msg);
});
