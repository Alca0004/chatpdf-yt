import { PineconeClient, Vector } from '@pinecone-database/pinecone';
import { downloadFromS3 } from './s3-server';
import { PDFLoader } from 'langchain/document_loaders/fs/pdf';
import {
  Document,
  RecursiveCharacterTextSplitter,
} from '@pinecone-database/doc-splitter';
import { getEmbeddings } from './embeddings';
import md5 from 'md5';

let pinecone: PineconeClient | null = null;

export const getPineconeClient = async () => {
  if (!pinecone) {
    pinecone = new PineconeClient();
    await pinecone.init({
      environment: process.env.PINECONE_ENVIRONMENT!,
      apiKey: process.env.PINECONE_API_KEY!,
    });
  }
  return pinecone;
};

type PDFPage = {
  pageContent: string;
  metaData: {
    loc: { pageNumber: number };
  };
};

export async function loadS3IntoPinecone(fileKey: string) {
  //1. Obtain the PDF
  console.log('Downloading s3 into file system');
  const file_name = await downloadFromS3(fileKey);
  if (!file_name) {
    throw new Error('could not download from s3');
  }
  const loader = new PDFLoader(file_name);
  const pages = (await loader.load()) as unknown as PDFPage[];

  //2. Split and segment as Pdf
  const documents = await Promise.all(pages.map(prepareDocument));

  //3. Vectorise and embed individual documents
  const vectors = await Promise.all(documents.flat().map(embedDocument));
}

async function embedDocument(doc: Document) {
  try {
    const embeddings = await getEmbeddings(doc.pageContent);
    const hash = md5(doc.pageContent);
    return {
      id: hash,
      values: embeddings,
      metadata: {
        text: doc.metadata.text,
        pageNumner: doc.metadata.pageNumber,
      },
    } as Vector;
  } catch (error) {
    console.log('error embedding documents', error);
    throw error;
  }
}

export const truncadeStringByBytes = (str: string, bytes: number) => {
  const enc = new TextEncoder();
  return new TextDecoder('utf-8').decode(enc.encode(str).slice(0, bytes));
};

async function prepareDocument(page: PDFPage) {
  let { pageContent, metaData } = page;
  pageContent = pageContent.replace(/\n/g, ' ');
  // Split the docs
  const splitter = new RecursiveCharacterTextSplitter();
  const docs = await splitter.splitDocuments([
    new Document({
      pageContent,
      metadata: {
        pageNumber: metaData.loc.pageNumber,
        text: truncadeStringByBytes(pageContent, 36000),
      },
    }),
  ]);
  return docs;
}

// npm install @pinecone-database/pinecone
// npm install langchain
// npm install pdf-parse
// npm install @pinecone-database/doc-splitter
// npm install md5
// npm install @types/md5
