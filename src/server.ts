import {
  GetObjectCommand,
  ObjectCannedACL,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import amqplib from "amqplib";
import sharp from "sharp";

export const s3Client = new S3Client({
  region: process.env.AWS_REGION!,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

export const bucketName = process.env.BUCKET_NAME;

const connection = await amqplib.connect(process.env.RABBIT_MQ_URL!);

const completedChannel = await connection.createChannel();
const toCompleteChannel = await connection.createChannel();

const completedImagesQueue = "completed-images";

completedChannel.assertQueue(completedImagesQueue, { durable: false });

const IMAGES_TO_PROCESS_EXCHANGE = "images-to-process-exchange";

await toCompleteChannel.assertExchange(IMAGES_TO_PROCESS_EXCHANGE, "fanout", {
  durable: false,
});

const { queue } = await toCompleteChannel.assertQueue("", {
  exclusive: true,
});

toCompleteChannel.bindQueue(queue, IMAGES_TO_PROCESS_EXCHANGE, "");

toCompleteChannel.consume(queue, async function (msg) {
  if (msg === null) return;

  console.log("Processor received %s", msg.content.toString());

  const message = JSON.parse(msg.content.toString());

  const { Body } = await s3Client.send(
    new GetObjectCommand({
      Bucket: bucketName,
      Key: message.fileName,
    })
  );

  const file = await Body?.transformToByteArray();

  const rotatedImage = await sharp(file).rotate(180).toBuffer();

  const processedFileName = `processed-${message.fileName}`;

  await new Promise((resolve) => setTimeout(resolve, 5_000));

  await s3Client.send(
    new PutObjectCommand({
      Bucket: bucketName,
      Key: processedFileName,
      Body: rotatedImage,
      ACL: ObjectCannedACL.public_read,
    })
  );

  const successMessage = {
    processedName: processedFileName,
    name: message.fileName,
  };

  completedChannel.sendToQueue(
    completedImagesQueue,
    Buffer.from(JSON.stringify(successMessage))
  );

  toCompleteChannel.ack(msg);
});

console.log("Worker started");
