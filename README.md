About
=====

This project allows you to create Lucene Indexes via a Lucene Directory object which uses Windows Azure BlobStorage for persistent storage.

This is java port of AzureDirectory for Lucene.Net https://azuredirectory.codeplex.com/

Supports: Lucene 4.9.0, Microsoft Azure Libraries for Java 0.5.0

You can download release here: https://javaazuredirectory.codeplex.com/

Usage
==============
To use you need to create a blobstorage account on http://azure.com

To add documents to catalog is as simple as:
```java

String StorageConnectionString = "DefaultEndpointsProtocol=http;" + 
			    "AccountName=<YOUR_ACCOUNT_NAME>;" + 
			    "AccountKey=<YOUR_API_KEY>";


CloudStorageAccount acc = CloudStorageAccount.parse(StorageConnectionString);
			
StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
AzureDirectory dir = new AzureDirectory(acc, "index", new File("./cache"));
			
IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
IndexWriter writer = new IndexWriter(dir, config);

Document doc = new Document();

doc.add(new TextField("title", "this is my title", Field.Store.YES));

writer.addDocument(doc);

writer.close();

```

And searching is as easy as:

```java

CloudStorageAccount acc = CloudStorageAccount.parse(StorageConnectionString);
			
StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
AzureDirectory dir = new AzureDirectory(acc, "index", new File("./cache"));
			
Query query = new QueryParser(Version.LUCENE_4_9, "title", analyzer).parse("my");
IndexReader reader = DirectoryReader.open(dir);
IndexSearcher searcher = new IndexSearcher(reader);
TopDocs result = searcher.search(query, 10);
			
for(ScoreDoc scoreDoc : result.scoreDocs) {
	System.out.println(reader.document(scoreDoc.doc).get("title"));
}
			
```

Caching
=======

By default AzureDirectory stores this local cache in a temporary directory (java.io.tmp variable). You can easily control where the local cache is stored by passing in a File object for whatever where location of storage what you want.

```java
AzureDirectory dir = new AzureDirectory(acc, "index", new File("./cache"));
```

