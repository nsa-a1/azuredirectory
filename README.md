About
=====

This project allows you to create Lucene Indexes via a Lucene Directory object which uses Windows Azure BlobStorage for persistent storage.

This is java port of AzureDirectory for Lucene.Net https://azuredirectory.codeplex.com/

Usage
==============
To use you need to create a blobstorage account on http://azure.com

To add documents to catalog is as simple as:
```java
need example
```

And searching is as easy as:

```java
need example
```

Caching
=======

By default AzureDirectory stores this local cache in a temporary directory (java.io.tmp variable). You can easily control where the local cache is stored by passing in a File object for whatever where location of storage what you want.

```java
need example
```

