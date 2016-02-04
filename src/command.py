import shlex

from tasks import subprocess, tempfile

def init(cmd, table="_version_" ):
  
  sql = (
    """
    CREATE TABLE IF NOT EXISTS `%s` (
      id INT(11) AUTO_INCREMENT PRIMARY KEY,
      version CHAR(14),
      description TEXT NULL,
      commit CHAR(40)
    );
    """
  ) % (table)

  return tempfile(sql) >> (lambda f: subprocess( shlex.split(cmd), f ) )



