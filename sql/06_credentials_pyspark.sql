USE master
CREATE LOGIN sparkLogin WITH PASSWORD = 'pyPass236@';

USE EcommerceFootballStore
CREATE USER sparkUser FOR LOGIN sparkLogin

ALTER ROLE db_datareader ADD MEMBER sparkUser
ALTER ROLE db_datawriter ADD MEMBER sparkUser
