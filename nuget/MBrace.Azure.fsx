﻿#I __SOURCE_DIRECTORY__
#r @"tools\Newtonsoft.Json.dll"
#r @"tools\FsPickler.Json.dll"
#r @"tools\FsPickler.dll"
#r @"tools\System.Spatial.dll"
#r @"tools\Mono.Cecil.dll"
#r @"tools\Vagabond.AssemblyParser.dll"
#r @"tools\Vagabond.dll"
#r @"tools\Microsoft.Data.Edm.dll"
#r @"tools\Microsoft.Data.OData.dll"
#r @"tools\Microsoft.Data.Services.Client.dll"
#r @"tools\Microsoft.ServiceBus.dll"
#r @"tools\Microsoft.WindowsAzure.Configuration.dll"
#r @"tools\Microsoft.WindowsAzure.Storage.dll"
#r @"tools\MBrace.Core.dll"
#r @"tools\MBrace.Runtime.dll"
#r @"tools\MBrace.Azure.dll"

open System.IO
open MBrace.Azure

AzureCluster.LocalWorkerExecutable <- Path.Combine(__SOURCE_DIRECTORY__, @"tools\mbrace.azureworker.exe")