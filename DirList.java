/*
 * List files in selected directories and find duplicate files.
 * 
 * Listed in an optional text file and a SQLite db.
 * 
 * Filters for drives, directories and files to be listed may be specified.
 * 
 * Easy detection of duplicate files using the calculated CRC and select groups in a DB browser (SQLite).
 * 
 * A base db my be created once and kept and additional directories passed against that base to find
 * duplicates on other drives or directories without need to refresh the original db.
 * 
 * Read the code to find all the places to specify options.
 */
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.zip.CRC32;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.output.NullWriter;

public class DirList {
    static final DirList x = new DirList();
    int progressCounter = 0;
        // User specified options
        static final boolean noTextDir = true; // suppress dir list in a file (only db output)
        static final boolean clearDB = false; // start clean or reuse previous DB of files
        // String[] rootDirectories= {/*"C:\\webserver", */"C:\\ProgramInstallations\\",
        //       "C:\\Users\\bike1\\", "F:\\"};
        static final String[] rootDirectories= {"D:\\"};
        static final String dirListFile = "dirlist.txt";
        static final String dbFile = "dirlist.db";
        
    public static void main(String[] args) throws Exception {

        try
         (PrintWriter textDirList =
              new PrintWriter(
                  noTextDir ? NullWriter.INSTANCE: new FileWriter(dirListFile));) {
          String driverTopLevelDomain = "org";
          String driverDomainNameAndSubProtocol = "sqlite";
          String protocol = "JDBC";
          
          // try to assure creation of fresh, new db file if requested
          // otherwise add new files to previous db.
          var db = new File(dbFile);
          if (db.exists()) {
            if (clearDB) {
              System.out.println("Successfully deleted previous db: " + db.delete() + "."); // force clean start for the db; could also use SQL delete, vacuum, truncate, drop
            }
            else {
              System.out.println("Adding to previous db.");
            }
          }
          else {
            System.out.println("No previous db.");
          }

          // Load the SQLite JDBC driver
          String databaseDriver = driverTopLevelDomain + "." + driverDomainNameAndSubProtocol + "." + protocol;
          Class.forName(databaseDriver);

          // Establish a connection to the database.
          String DBurl = protocol + ":" + driverDomainNameAndSubProtocol + ":" + dbFile;
          try (
              Connection DBconnection = DriverManager.getConnection(DBurl); // connect to the DB. If the database file doesn't exist, it will be created.
              Statement unpreparedStatement = DBconnection.createStatement(); // Create a Statement object for executing SQL queries empty utility not parameterized SQL statement set as needed
          ) {
              System.out.println("Database opened successfully.");

              // add the requested directory listings to the db
              try {
              // If this fails with SQLITE_BUSY or SQLITE_LOCKED, the database is locked.
              var checkLockedLock = "BEGIN IMMEDIATE";
              var clearCheckLockedLock = "ROLLBACK";
              unpreparedStatement.execute(checkLockedLock);
              unpreparedStatement.execute(clearCheckLockedLock);

              // Create a table if it doesn't exist
              var createTableSQL = "CREATE TABLE IF NOT EXISTS files (crc LONG, name TEXT, path TEXT, size LONG);";
              unpreparedStatement.executeUpdate(createTableSQL);
              System.out.println("Table created."); // or could mean already exists but this program initially deletes the db

              // start one big transaction that ends with a commit (or rollback if something goes wrong)
              var startTransaction = "BEGIN TRANSACTION";
              unpreparedStatement.executeUpdate(startTransaction);
              System.out.println("Db initialized.");
  
              Filewalker fw = x.new Filewalker(textDirList, DBconnection);

              for (var rootDirectory : rootDirectories) {
                System.out.println("Start finding desired (filtered) files in " + rootDirectory + ".");
                fw.walk(rootDirectory); // search for files and directories
              }
              // successfully completed so commit all the inserts from the directories' walks
              var endGoodTransaction = "COMMIT"; // all or nothing - no db COMMIT until program ends okay
              unpreparedStatement.executeUpdate(endGoodTransaction);
              System.out.println("\n\nDesired files inserted into db.");
              } catch (Exception e) {
                e.printStackTrace();
                var endBadTransaction = "ROLLBACK";
                unpreparedStatement.execute(endBadTransaction);
              }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          finally {
            System.out.println("\nDone.");
          }
        }
    }

  public class Filewalker {
    PrintWriter textDirList;
    Connection DBconnection;
    PreparedStatement insertStatement;

    Filewalker(PrintWriter textDirList, Connection DBconnection) {
    this.textDirList = textDirList;
    this.DBconnection = DBconnection;

    // Insert data using a prepared statement
    String insertSQLparameterized = "INSERT INTO files(crc, name, path, size) VALUES(?, ?, ?, ?)";
    try  {
      insertStatement = DBconnection.prepareStatement(insertSQLparameterized);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    }

    /**
     * Called recursively for subdirectories
     * @param path Starting path that is searched to the end of the tree
     * @throws IOException 
     */
    public void walk(String startPath) throws IOException {

      File root = new File(startPath);

      // Create an inline Filename Filter
      FileFilter filter = new FileFilter() {
        public boolean accept(File file) // or accept(File f, String name)
        {
          // System.out.print(file.isDirectory()?"DIR: ":file.isFile()?"FILE: ":"NONE");
          // System.out.println(file + " <>" + file.getAbsoluteFile().toString() + "{}" +
          // file.getAbsolutePath() + "  " + file.canRead());

          return
            (!file.isHidden())
              &&
            (
              (file.isDirectory()
                // user option to suppress any directories to list
                &&
                !file.getAbsoluteFile().toString().endsWith("_files") // but not the directory of a saved web page
                &&
                !file.getAbsoluteFile().toString().endsWith("\\AppData") // and not \AppData directories
                &&
                !file.getAbsoluteFile().toString().contains("\\.") // and not \. directories
                &&
                !file.getAbsoluteFile().toString().startsWith("C:\\Windows")
                &&
                !file.getAbsoluteFile().toString().startsWith("C:\\Users\\Public\\wpilib\\")
                &&
                !file.getAbsoluteFile().toString().startsWith("C:\\opencv\\")
                &&
                !file.getAbsoluteFile().toString().startsWith("C:\\Program Files")
              ) // process all directories as requested
            ||
              (file.isFile()  // process selected readable files
            //   &&
            //   ( // ending with: 
            //     (file.getName().length() >= 4
            //       && 
            //       (file.getName().substring(file.getName().length() - 4).equalsIgnoreCase(".JPG")
            //       ||
            //       file.getName().substring(file.getName().length() - 4).equalsIgnoreCase(".PNG"))
            //     )
            //     ||
            //     (file.getName().length() >= 5
            //       &&
            //       file.getName().substring(file.getName().length() - 5).equalsIgnoreCase(".JPEG")
            //     )
            //   )
              // user option to suppress any file types or files to list
              && !FilenameUtils.isExtension(file.getName().toLowerCase(), // filter out unneeded files
              "bin", "js", "lock", "dll", "class", "json", "md", "sys", "pdb", "gradle", "mk", "prefs")
              && !file.getName().toLowerCase().contains("licen")
              )
          )
            ;
        }
      }; // ! Create a Filename Filter


      File[] list = root.listFiles(filter);

      if (list == null)
        return;

      for (File file : list) {
          // System.out.print(file.isDirectory()?"DIR: ":file.isFile()?"FILE: ":"NONE");
          // System.out.println(file + " <>" + file.getAbsoluteFile().toString() + "{}" +
          // file.getAbsolutePath() + "  " + file.canRead());
        if (++progressCounter%50 == 0) System.out.print(progressCounter + "\r");

        if (file.isDirectory())
        {
          // out.println( file.getAbsoluteFile().toString() + "<" + " recurse");
          walk(file.getAbsoluteFile().toString()); // recursive invocation to subdirectory
        }
        else
        {
          BasicFileAttributes attributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
          var crc = calculateCRC32(file.toString());
          var name = file.getName();
          var path = file.getParent();
          var size = attributes.size();
          textDirList.format("%d, \"%s\", \"%s\", %d%n",
            crc, name, path, size);
            try {
              insertStatement.setLong(1, crc);
              insertStatement.setString(2, name);
              insertStatement.setString(3, path);
              insertStatement.setLong(4, size);
              insertStatement.executeUpdate();
            } catch (SQLException e) {
              e.printStackTrace();
            }
          //   System.out.println("File size: " + attributes.size() + " bytes");
          //   System.out.println("Creation time: " + attributes.creationTime());
          //   System.out.println("Last access time: " + attributes.lastAccessTime());
          //   System.out.println("Last modified time: " + attributes.lastModifiedTime());
          //   System.out.println("Is directory: " + attributes.isDirectory());
          //   System.out.println("Is regular file: " + attributes.isRegularFile());
          //   System.out.println("Is symbolic link: " + attributes.isSymbolicLink());
          //   System.out.println("Is other: " + attributes.isOther());
        }
      }
      // the end of the file list for this directory
      // recursive invocations unwind here at the end
    }
  }

  public static long calculateCRC32(String filePath) throws IOException {
    CRC32 crc32 = new CRC32();
    byte[] buffer = new byte[8192*8]; // Use a buffer for efficient reading
    int bytesRead;

    try (FileInputStream fis = new FileInputStream(filePath)) {
        while ((bytesRead = fis.read(buffer)) != -1) {
            crc32.update(buffer, 0, bytesRead);
        }
    } catch(Exception e) {
      System.out.println(e.getMessage().replace("\\u", "\\\\u").replace("\\U", "\\\\U")); // if output is used elsewhere, get rid of invalid unicode error
      crc32.reset(); // any caught errors return a 0 crc; usually it's access denied at the open which has crc = 0 initially anyway
              // This makes the 0 crc look a little odd as several very different and different sized files get the same 0 crc.
    }

    return crc32.getValue();
  }
}
/*
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FilenameUtils;

public class FilenameMethods {

    public static void main(String[] args) throws IOException {
        File file = new File("/path/to/my/file.txt");
        Path path = Paths.get("/path/to/my/file.txt");

        System.out.println("File name (File): " + file.getName());
        System.out.println("Absolute path (File): " + file.getAbsolutePath());
        System.out.println("Canonical path (File): " + file.getCanonicalPath());
        System.out.println("Parent directory (File): " + file.getParent());

        System.out.println("File name (Path): " + path.getFileName());
        System.out.println("Parent directory (Path): " + path.getParent());
        System.out.println("Absolute path (Path): " + path.toAbsolutePath());
        System.out.println("Real path (Path): " + path.toRealPath());

        System.out.println("Base name (FilenameUtils): " + FilenameUtils.getBaseName(file.getName()));
        System.out.println("Extension (FilenameUtils): " + FilenameUtils.getExtension(file.getName()));
    }
}
 */
// cd C:\Users\bike1\FRC\2025\DirList
// sort dirlist.txt /O dirlist.csv

// select crc, name, path, size
// from files
// where crc in
//   (select crc from files group by crc having count(*) > 1)
// order by size desc

// select crc, name, path
// from files
// where crc in
//    (select crc from files group by crc having count(*) > 1)
//  and path not like "C:%"
// order by path COLLATE NOCASE, name COLLATE NOCASE

// delete from files where path like "F:%"   
// rollback
// commit

// select soundex(name), crc, name, path, size
// from files
// group by soundex(name)


// select crc, name, path
// from files
// where name in
//    (select name from files group by name having count(*) > 1)
//    and path not like "C:%"
//    order by path COLLATE NOCASE, name COLLATE NOCASE

// DosFileAttributeView dosAttributes = Files.getFileAttributeView(file.toPath(), DosFileAttributeView.class);
// if (dosAttributes != null) {
//     DosFileAttributes attrs = dosAttributes.readAttributes();
//     boolean isReadOnly = attrs.isReadOnly();
//     boolean isHidden = attrs.isHidden();
//     boolean isSystem = attrs.isSystem();
//     boolean isArchive = attrs.isArchive();
//     // ... further processing of attributes
// }

/*

[NOTE most back slash u or back slash U changed to \\u or \\U otherwise invalid unicode error]

Adding to previous db.
Database opened successfully.
Table created.
Db initialized.
Start finding desired (filtered) files in F:\.
1350

Desired files inserted into db.

Done.
*/
