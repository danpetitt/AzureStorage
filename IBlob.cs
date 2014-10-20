// <copyright file="IBlob.cs" company="CodeRanger.com">
//     CodeRanger.com and Dan Petitt. All rights reserved.
// </copyright>
// <author>Dan Petitt</author>

namespace AzureStorage
{
  using System;
  using System.Diagnostics.CodeAnalysis;
  using System.IO;
  using System.Runtime.InteropServices;
  using System.Text;
  using Microsoft.WindowsAzure.Storage;
  using Microsoft.WindowsAzure.Storage.Auth;
  using Microsoft.WindowsAzure.Storage.Blob;
  using Newtonsoft.Json;

  [Guid("BCC86C64-8A61-47F4-B653-0A95A031A137")]

  /// <summary>
  /// Provides COM interface to our Blob class for managing a blob in Azure Storage.
  /// <para />
  /// Call Initialize first to setup the Azure account, then you can use the other methods to perform actions on your Storage containers
  /// </summary>
  public interface IBlob
  {
    /// <summary>
    /// Gets or sets the account name for an Azure Storage account, not needed if UseDevelopmentStorage field is set
    /// </summary>
    [DispId(1)]
    string AccountName { get; set; }

    /// <summary>
    /// Gets or sets the account key for an Azure Storage account, not needed if UseDevelopmentStorage field is set
    /// </summary>
    [DispId(2)]
    string AccountKey { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to use local development storage instead of an Azure Storage account
    /// </summary>
    [DispId(3)]
    bool UseDevelopmentStorage { get; set; }

    /// <summary>
    /// Initializes the blob storage account with the given account settings
    /// </summary>
    [DispId(4)]
    void Initialize();

    /// <summary>
    /// Allows you to upload a block blob to Azure storage using account settings when initialized 
    /// </summary>
    /// <param name="containerName">The container name to use or automatically create for containing the blob uploaded</param>
    /// <param name="fullPathToFileForUpload">The local file path of the file to upload</param>
    /// <param name="fileType">Optional file mime-type, for example application/ms-excel for Excel file, defaults to application/x-octet-stream if not provided</param>
    /// <returns>The URI of the blob in Azure Storage</returns>
    [DispId(5)]
    string UploadBlockBlob( string containerName, string fullPathToFileForUpload, string fileType = "application/x-octet-stream" );

    /// <summary>
    /// Returns an array of blobs in given container
    /// </summary>
    /// <param name="containerName">The container name to use or automatically create for investigating</param>
    /// <returns>An array of blob URIs in the given container</returns>
    [DispId(6)]
    string GetBlobsInContainer( string containerName );

    /// <summary>
    /// Downloads a blob with the given complete URI to a local file path
    /// </summary>
    /// <param name="uri">Blob URI which you wish to delete</param>
    /// <param name="pathToFileForDownload">Local path in which the blob is saved. This is the path and does not include the filename which is automatically added with the blob name</param>
    [DispId(7)]
    void DownloadBlob( string uri, string pathToFileForDownload );

    /// <summary>
    /// Deletes a blob with a given complete URI
    /// </summary>
    /// <param name="uri">Blob URI which you wish to delete</param>
    [DispId(8)]
    void DeleteBlob( string uri );
  }

  [ClassInterface( ClassInterfaceType.None )]
  [ComVisible( true )]

  /// <summary>
  /// Class interface for managing a blob in Azure Storage
  /// </summary>
  public class Blob : IBlob
  {
    /// <summary>
    /// Private property to store the current Azure storage account created on Initialize
    /// </summary>
    [SuppressMessage( "StyleCop.CSharp.NamingRules", "SA1309:FieldNamesMustNotBeginWithUnderscore", Justification = "Reviewed." )]
    private CloudStorageAccount _storageAccount;

    /// <summary>
    /// Private property to store the account key for an Azure Storage account
    /// </summary>
    [SuppressMessage( "StyleCop.CSharp.NamingRules", "SA1309:FieldNamesMustNotBeginWithUnderscore", Justification = "Reviewed." )]
    private string _storageAccountKey = string.Empty;

    /// <summary>
    /// Private property to store the account name for an Azure Storage account
    /// </summary>
    [SuppressMessage( "StyleCop.CSharp.NamingRules", "SA1309:FieldNamesMustNotBeginWithUnderscore", Justification = "Reviewed." )]
    private string _storageAccountName = string.Empty;

    /// <summary>
    /// Private property to store the value for using development storage instead of an Azure Storage account
    /// </summary>
    [SuppressMessage( "StyleCop.CSharp.NamingRules", "SA1309:FieldNamesMustNotBeginWithUnderscore", Justification = "Reviewed." )]
    private bool _storageUseDevelopmentStorage = false;

    /// <summary>
    /// Gets or sets the account name for an Azure Storage account, not needed if UseDevelopmentStorage field is set
    /// </summary>
    public string AccountName
    {
      get
      {
        return this._storageAccountName;
      }

      set
      {
        this._storageAccountName = value;
      }
    }

    /// <summary>
    /// Gets or sets the account key for an Azure Storage account, not needed if UseDevelopmentStorage field is set
    /// </summary>
    public string AccountKey
    {
      get
      {
        return this._storageAccountKey;
      }

      set
      {
        this._storageAccountKey = value;
      }
    }

    /// <summary>
    /// Gets or sets a value indicating whether to use local development storage instead of an Azure Storage account
    /// </summary>
    public bool UseDevelopmentStorage
    {
      get
      {
        return this._storageUseDevelopmentStorage;
      }

      set
      {
        this._storageUseDevelopmentStorage = value;
      }
    }

    /// <summary>
    /// Initializes the blob storage account with the account settings set by the AccountName and AccountKey fields
    /// </summary>
    public void Initialize()
    {
      if( !this.UseDevelopmentStorage )
      {
        if( this.AccountName.Length == 0 )
        {
          throw new COMException( "Error initializing the Azure Storage Account because accountName parameter is empty" );
        }
        else if( this.AccountKey.Length == 0 )
        {
          throw new COMException( "Error initializing the Azure Storage Account because accountKey parameter is empty" );
        }
      }

      try
      {
        this._storageAccount = this.UseDevelopmentStorage ? CloudStorageAccount.DevelopmentStorageAccount : new CloudStorageAccount( new StorageCredentials( this.AccountName, this.AccountKey ), true );
      }
      catch( System.Exception ex )
      {
        throw new COMException( "Error initializing the Azure Storage Account because: " + ex.Message );
      }
    }

    /// <summary>
    /// Allows you to upload a block blob to Azure storage using account settings when initialized 
    /// </summary>
    /// <param name="containerName">The container name to use or automatically create for containing the blob uploaded</param>
    /// <param name="fullPathToFileForUpload">The local file path of the file to upload</param>
    /// <param name="fileType">Optional file mime-type, for example application/ms-excel for Excel file, defaults to application/x-octet-stream if not provided</param>
    /// <returns>The URI of the blob in Azure Storage</returns>
    public string UploadBlockBlob( string containerName, string fullPathToFileForUpload, string fileType = "application/x-octet-stream" /*, bool isPublic*/ )
    {
      try
      {
        Console.WriteLine( "> Uploading BlockBlob" );

        // Create the blob client
        CloudBlobClient blobClient = this._storageAccount.CreateCloudBlobClient();

        // create or find our container references
        CloudBlobContainer container = GetContainer( blobClient, containerName, new BlobContainerPermissions{ PublicAccess = BlobContainerPublicAccessType.Blob } );
      
        // Get our block reference and upload data from the given filepath      
        CloudBlockBlob blockBlob = container.GetBlockBlobReference( Path.GetFileName( fullPathToFileForUpload ) );
        blockBlob.Properties.ContentType = fileType;

        using( var filestream = File.OpenRead( fullPathToFileForUpload ) )
        {
          blockBlob.UploadFromStream( filestream );
        }

        Console.WriteLine( "\t Blob is now available at {0}", blockBlob.Uri.ToString() );

        return blockBlob.Uri.ToString();
      }
      catch( System.Exception ex )
      {
        throw new COMException( "Error uploading block blob because: " + ex.Message );
      }
    }

    /// <summary>
    /// Returns an array of blobs in given container
    /// </summary>
    /// <param name="containerName">The container name to use or automatically create for investigating</param>
    /// <returns>An array of blob URIs in the given container</returns>
    public string GetBlobsInContainer( string containerName )
    {
      Console.WriteLine( "> List Blobs" );

      try
      {
        CloudBlobClient blobClient = this._storageAccount.CreateCloudBlobClient();

        CloudBlobContainer container = blobClient.GetContainerReference( containerName );

        StringBuilder sb = new StringBuilder();
        StringWriter sw = new StringWriter( sb );
        using( JsonWriter writer = new JsonTextWriter( sw ) )
        {
          writer.Formatting = Formatting.None;

          writer.WriteStartArray();

          foreach( IListBlobItem blob in container.ListBlobs( null, false ) )
          {
            // Blob type will be CloudBlockBlob, CloudPageBlob or CloudBlobDirectory
            // use blob.GetType() and cast to appropriate type to gain access to properties specific to each type
            Console.WriteLine( "\t {0} {1} \t {2}", blob.GetType(), Environment.NewLine, blob.Uri );

            if( blob.GetType() == typeof( CloudBlockBlob ) )
            {
              writer.WriteStartObject();

              CloudBlockBlob cloudBlobItem = new CloudBlockBlob( blob.Uri );
              cloudBlobItem.FetchAttributes();

              writer.WritePropertyName( "name" );
              writer.WriteValue( cloudBlobItem.Name );

              writer.WritePropertyName( "uri" );
              writer.WriteValue( cloudBlobItem.Uri.ToString() );

              writer.WritePropertyName( "type" );
              writer.WriteValue( cloudBlobItem.Properties.BlobType.ToString() );

              writer.WritePropertyName( "lastModified" );
              writer.WriteValue( cloudBlobItem.Properties.LastModified.ToString() );

              writer.WritePropertyName( "contentType" );
              writer.WriteValue( cloudBlobItem.Properties.ContentType );

              writer.WritePropertyName( "contentMD5" );
              writer.WriteValue( cloudBlobItem.Properties.ContentMD5.ToString() );

              writer.WritePropertyName( "size" );
              writer.WriteValue( cloudBlobItem.Properties.Length.ToString() );

              writer.WriteEndObject();
            }
          }

          writer.WriteEndArray();
        }

        return sb.ToString();
      }
      catch( System.Exception ex )
      {
        throw new COMException( "Error getting container blobs because: " + ex.Message );
      }

      // foreach( IListBlobItem blob in container.ListBlobs( null, true ) )
      // {
      //   //Blob type will be CloudBlockBlob, CloudPageBlob or CloudBlobDirectory
      //   // use blob.GetType() and cast to appropriate type to gain access to properties specific to each type
      //   Console.WriteLine( "\t {0} {1} \t {2}", blob.GetType(), Environment.NewLine, blob.Uri );
      // }
    }

    /// <summary>
    /// Downloads a blob with the given complete URI to a local file path
    /// </summary>
    /// <param name="uri">Blob URI which you wish to delete</param>
    /// <param name="pathToFileForDownload">Local file path in which the blob is saved</param>
    public void DownloadBlob( string uri, string pathToFileForDownload )
    {
      Console.WriteLine( "> Download Blob from {0}", uri.ToString() );

      try
      {
        CloudBlobClient blobClient = this._storageAccount.CreateCloudBlobClient();

        ICloudBlob blobItem = blobClient.GetBlobReferenceFromServer( new Uri( uri ) );

        string downloadToPath = string.Format( "{0}/{1}", pathToFileForDownload, blobItem.Name );
        using( var fs = File.OpenWrite( downloadToPath ) )
        {
          blobItem.DownloadToStream( fs );
          Console.WriteLine( "\t Blob downloaded to file: {0}", downloadToPath );
        }

        // //Demonstrate how to download a blob from uri to a MemoryStream
        // using ( var ms = new MemoryStream() )
        // {
        //   blobItem.DownloadToStream( ms );
        // 
        //   //Now process the memory stream however you like        
        //   Console.WriteLine( "\t Now process the memory stream however you like. Memory Stream Length: {0}", ms.Length );
        // }
      }
      catch( System.Exception ex )
      {
        throw new COMException( "Error downloading blob " + uri + " to " + pathToFileForDownload + " because: " + ex.Message );
      }
    }

    /// <summary>
    /// Deletes a blob with a given complete URI
    /// </summary>
    /// <param name="uri">Blob URI which you wish to delete</param>
    public void DeleteBlob( string uri )
    {
      Console.WriteLine( "> Delete Blob" );

      try
      {
        CloudBlobClient blobClient = this._storageAccount.CreateCloudBlobClient();

        ICloudBlob blobItem = blobClient.GetBlobReferenceFromServer( new Uri( uri ) );
        if( blobItem != null )
        {
          var success = blobItem.DeleteIfExists();

          Console.WriteLine( "\t {0} Deleting Blob {1}", success ? "Successful" : "Unsuccessful", uri.ToString() );
        }
      }
      catch( System.Exception ex )
      {
        throw new COMException( "Error deleting blob " + uri + " because: " + ex.Message );
      }
    }

    /// <summary>
    /// Private method to get a container reference from a container name. If the container does not exist, it is created with the given permissions.
    /// </summary>
    /// <param name="blobClient">Reference to a CloudBlobClient for this storage account</param>
    /// <param name="containerName">The container name to use or automatically create</param>
    /// <param name="permissions">Permissions object to use if the container needs to be created</param>
    /// <returns>A reference to the existing or new storage container</returns>
    private static CloudBlobContainer GetContainer( CloudBlobClient blobClient, string containerName, BlobContainerPermissions permissions )
    {
      Console.WriteLine( "> Create Container '{0}' and Set Permissions to {1}", containerName, permissions.ToString() );

      // Retrieve a reference to a container
      CloudBlobContainer container = blobClient.GetContainerReference( containerName.ToLower() );

      // Create the container if it doesn't already exist
      container.CreateIfNotExists();

      // Set the container permissions
      container.SetPermissions( permissions );

      return container;
    }
  }
}
