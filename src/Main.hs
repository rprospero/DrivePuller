{-# LANGUAGE OverloadedStrings, TemplateHaskell #-}

import Network.Google.OAuth2 (OAuth2Client(OAuth2Client),getAccessToken)
import Network.Google.Drive (driveScopes)
import Network.Google.Api (runApi,Api,ApiError,sinkFile)
import Network.Google.Drive.Search (listFiles,qIn,qContains,Field(Parents,Title),(?&&))
import Network.Google.Drive.File (newFile,getFile,fileId,File(fileData),setParent,FileData,downloadFile)
import Network.Google.Drive.Upload (createFileWithContent,uploadSourceFile,updateFileWithContent)
import Data.Conduit (($$+-))
import Data.String (fromString)
import Data.List.Split (chunksOf)
import Data.Text (Text)
import Data.Foldable (foldrM)
import Filesystem (getSize)
import Control.Monad.Trans.Class (lift)
import System.IO
import Control.Exception (bracket)
import Text.Printf
import Control.Monad.Parallel as P
import System.Directory (doesFileExist)
import System.FilePath (splitDirectories)

import System.Environment (getArgs)
import Data.Monoid
import Options.Applicative
import Data.FileEmbed (embedStringFile)

getFolder :: [Text] -> Api File
getFolder path = do
  Just root <- getFile "root"
  foldrM descendTree root $ reverse path

childOf x = fileId x `qIn` Parents

descendTree :: Text -> File -> Api File
descendTree label branch = do
  children <- listFiles $ childOf branch ?&& Title `qContains` label
  return $ head children

fetchFile :: File -> String -> Api ()
fetchFile dir label = do
  f <- listFiles $ childOf dir ?&& Title `qContains` (fromString label :: Text)
  Prelude.mapM_ (`downloadFile` ($$+- sinkFile label)) f

fileNames :: Runset -> [FilePath]
fileNames r = map (printf (pattern r)) [start r..stop r]

main' :: Runset -> IO (Either ApiError ())
main' r = do
  let serverKey = $(embedStringFile "client.dat")
  let client = OAuth2Client clientId serverKey

  token <- getAccessToken client driveScopes $ Just "token.dat"

  let _:dirPath = map fromString . splitDirectories . directory $ r
  print dirPath

  Right folder <- runApi token . getFolder $ dirPath

  print $ fileNames r
  print folder

  runApi token $ P.mapM_ (fetchFile folder) $ fileNames r

data Runset = Runset {start :: Int
                     ,stop :: Int
                     ,pattern :: String
                     ,directory :: FilePath}

main = execParser opts >>= main'
       where
         opts = info (helper <*> parse)
                ( fullDesc
                <> progDesc "Download a set of runs from the Google Drive"
                <> header "DrivePuller - a command line utility for Google Drive")

parse = Runset <$> (read <$> strOption (long "start"
                                       <> metavar "RUNNUMBER"
                                       <> help "The first run to upload"))
        <*> (read <$> strOption (long "stop"
                             <> metavar "RUNNUMBER"
                             <> help "The last run to upload"))
        <*> (strOption (long "pattern"
                    <> metavar "NAMEPATTERN"
                    <> help "A pattern for the file name.  Use %05d to stand in for the number"))
        <*> (strOption (long "directory"
                    <> metavar "DIRECTORYPATH"
                    <> help "The location in the google drive where the files are located.  For example, us \"/data/october 2015/\" to get the files from the october 2015 subfolder of the data folder."))

clientId = "229144286373-ful740p31095qlnfqa50oa8onoaqo35k.apps.googleusercontent.com"
