import time
import mysql.connector as mariadb

def clearDatabase():
    try:
        sql="SET FOREIGN_KEY_CHECKS=0"
        cursor.execute(sql,)
        for tab in ["customers","employee","gamereservation","product","gamesales","reservationlist","saleprice","saleslist"]:
            sql="DROP TABLE "
            cursor.execute(sql+tab,)
        sql="SET FOREIGN_KEY_CHECKS=1"
        cursor.execute(sql,)
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(e)


# In[182]:


def createTables():
    try:
        ddl_statements = [
            '''CREATE TABLE Product (
                pName VARCHAR(200) NOT NULL,
                availableStock INTEGER NOT NULL CHECK(availableStock >= 0),
                price REAL NOT NULL CHECK(price >= 0),
                reservedCopies INTEGER NOT NULL CHECK(reservedCopies >= 0),
                PRIMARY KEY (pName)
            );''',

            '''CREATE TABLE Employee (
                eID CHAR(7) NOT NULL,
                fName VARCHAR(30) NOT NULL,
                lName VARCHAR(30) NOT NULL,
                phoneNum VARCHAR(20),
                PRIMARY KEY (eID)
            );''',

            '''CREATE TABLE Customers (
                cFName VARCHAR(30) NOT NULL,
                cLName VARCHAR(30) NOT NULL,
                cEmail VARCHAR(320) NOT NULL,
                PRIMARY KEY (cEmail)
            );''',

            '''CREATE TABLE ReservationList (
                reservationID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
                cEmail VARCHAR(320) NOT NULL,
                timestamp DATETIME NOT NULL,
                FOREIGN KEY (cEmail)
                REFERENCES Customers(cEmail) ON UPDATE CASCADE ON DELETE CASCADE
            );''',

            '''CREATE TABLE gameReservation (
                reservationID CHAR(7) NOT NULL,
                pName VARCHAR(200) NOT NULL,
                copiesReserved INTEGER CHECK(copiesReserved >= 0),
                PRIMARY KEY (reservationID, pName),
                FOREIGN KEY (pName)
                REFERENCES Product(pName) ON UPDATE CASCADE ON DELETE CASCADE
            );''',

            '''CREATE TABLE SalesList (
                saleID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
                cEmail VARCHAR(320) NOT NULL,
                timestamp DATETIME NOT NULL,
                eID CHAR(7) NOT NULL,
                FOREIGN KEY (cEmail)
                REFERENCES Customers(cEmail) ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (eID)
                REFERENCES Employee(eID) ON UPDATE CASCADE ON DELETE CASCADE
            );''',

            '''CREATE TABLE gameSales (
                saleID CHAR(7) NOT NULL,
                pName VARCHAR(200) NOT NULL,
                copiesSold INTEGER NOT NULL CHECK(copiesSold >= 0),
                PRIMARY KEY (saleID, pName),
                FOREIGN KEY (pName)
                REFERENCES Product(pName) ON UPDATE CASCADE ON DELETE CASCADE
            );''',

            '''CREATE TABLE SalePrice (
                saleID CHAR(7) NOT NULL,
                pName VARCHAR(200) NOT NULL,
                salePrice REAL NOT NULL CHECK(salePrice >= 0),
                PRIMARY KEY (saleID, pName),
                FOREIGN KEY (pName)
                REFERENCES Product(pName) ON UPDATE CASCADE ON DELETE CASCADE
            );'''
        ]

        for statement in ddl_statements:
            cursor.execute(statement)
        connection.commit()
    except Exception as e:
            connection.rollback()
            print(e)

def productCreate(pName, stock, price, reservedCopies):
    try:
        sql= "Insert INTO Product(pName,availableStock,price,reservedCopies)VALUES (%s,%s,%s,%s) "
        data= (pName, stock, price,reservedCopies)
        cursor.execute(sql, data)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(e)
        return False

def productStock(pName, qty):
    try:
        sql= "UPDATE Product SET availableStock=availableStock+%s WHERE pName=%s"
        data= (qty,pName)
        cursor.execute(sql, data)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(e)
        return False

def productSetStock(pName, qty):
    try:
        sql= "UPDATE Product SET availableStock=%s WHERE pName=%s"
        data= (qty,pName)
        cursor.execute(sql, data)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(e)
        return False

def productChange(oldPName,newPName=None,newPrice=None):
    try:
        sql=""
        if newPName!=None:
            sql+="SET pName=%s"
            data=newPName
            if newPrice !=None:
                sql+="AND price=%s"
                data=(newPName,newPrice)
        elif newPrice!=None:
            sql+="SET price=%s"
            data=newPrice
        else:
            return False
        cursor.execute(sql, data)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(e)
        return False

def refund(pName, saleID):
    try:
        sql= "SELECT copiesSold FROM gameSales Where pName=%s AND saleID=%s"
        data= (pName,saleID)
        ret=cursor.execute(sql, data)
        soldQty=cursor.fetchone()[0][0]
        sql= "SELECT salePrice FROM SalePrice Where pName=%s AND saleID=%s"
        ret=cursor.execute(sql, data)
        soldPrice=cursor.fetchone()[0][0]
        sql="DELETE FROM gameSales WHERE pName=%s AND saleID=%s"
        ret=cursor.execute(sql, data)
        sql="UPDATE Product SET availableStock=availableStock+%s WHERE pName=%s"
        data=(soldQty,pName)
        ret=cursor.execute(sql, data)
        sql="DELETE FROM SalesList WHERE NOT EXISTS (SELECT * FROM GameSales WHERE saleID=%s)"
        data=(saleID,)
        ret=cursor.execute(sql, data)
        connection.commit()
        return soldPrice*soldQty
    except Exception as e:
        connection.rollback()
        print(e)
        return False

def createEmployee(eID,fName,lName,phoneNum):
    try:
        sql= "INSERT INTO EMPLOYEE(eID, fName, lName, phoneNum) VALUES(%s,%s,%s,%s)"
        data= (eID,fName,lName,phoneNum)
        ret=cursor.execute(sql, data)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        return False
def createCustomer(cEmail, cFName, cLName):
    try:
        sql= "INSERT INTO CUSTOMERS(cEmail, cFName, cLName) VALUES(%s,%s,%s)"
        data= (cEmail, cFName, cLName)
        ret=cursor.execute(sql, data)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(e)
        return False

def createSale(pName,qty,cEmail,eID):
    try:
        sql= "UPDATE Product Set availableStock=AvailableStock-%s where pName=%s"
        data= (qty,pName)
        ret=cursor.execute(sql, data)
        sql= "INSERT INTO SalesList(cEmail,timestamp,eID)Values (%s,NOW(),%s)"
        data= (cEmail,eID)
        ret=cursor.execute(sql, data)
        sql= "SELECT saleID from SalesList ORDER BY saleID DESC limit 1"
        ret=cursor.execute(sql, )
        data= (cEmail,eID)
        saleID=cursor.fetchone()[0]
        sql= "INSERT INTO gameSales(saleID,pName,copiesSold)Values (%s,%s,%s)"
        data=(saleID,pName,qty)
        ret=cursor.execute(sql, data)
        sql= "SELECT price from product where pname=%s"
        ret=cursor.execute(sql, (pName,))
        data= (cEmail,eID)
        salePrice=cursor.fetchone()[0]
        sql= "INSERT INTO SalePrice(saleID,pName,salePrice)Values (%s,%s,%s)"
        data=(saleID,pName,salePrice)
        ret=cursor.execute(sql, data)
        connection.commit()
        return saleID
    except Exception as e:
        connection.rollback()
        print(e)
        return False
def sale(pList,qtyList, cEmail, eID):
    argList=list(zip(pList, qtyList))
    saleID=createSale(argList[0][0],argList[0][1],cEmail,eID)
    if saleID==False:
        print("Initial sale failed")
        return False
    for x in argList[1:]:
        pName=x[0]
        qty=x[1]
        try:
            sql= "UPDATE Product Set availableStock=AvailableStock-%s where pName=%s"
            data= (qty,pName)
            ret=cursor.execute(sql, data)
            sql= "INSERT INTO gameSales(saleID,pName,copiesSold)Values (%s,%s,%s)"
            data=(saleID,pName,qty)
            ret=cursor.execute(sql, data)
            sql= "SELECT price from product where pname=%s"
            ret=cursor.execute(sql, (pName,))
            data= (cEmail,eID)
            salePrice=cursor.fetchone()[0]
            sql= "INSERT INTO SalePrice(saleID,pName,salePrice)Values (%s,%s,%s)"
            data=(saleID,pName,salePrice)
            ret=cursor.execute(sql, data)
            connection.commit()
        except Exception as e:
            connection.rollback()
            print("Sale errored for "+x)
            print(e)
            return False
    return True
def createReservation(pName,qty,cEmail):
    try:
        sql= "UPDATE Product Set availableStock=AvailableStock-%s where pName=%s"
        data= (qty,pName)
        ret=cursor.execute(sql, data)
        sql= "UPDATE Product Set reservedCopies=reservedCopies+%s where pName=%s"
        data= (qty,pName)
        ret=cursor.execute(sql, data)
        sql= "INSERT INTO ReservationList(cEmail,timestamp)Values (%s,NOW())"
        data= (cEmail,)
        ret=cursor.execute(sql, data)
        sql= "SELECT reservationID from ReservationList ORDER BY reservationID DESC limit 1"
        ret=cursor.execute(sql, )
        reservationID=cursor.fetchone()[0]
        sql= "INSERT INTO gameReservation(reservationID,pName,copiesReserved)Values (%s,%s,%s)"
        data=(reservationID,pName,qty)
        ret=cursor.execute(sql, data)
        connection.commit()
        return reservationID
    except Exception as e:
        connection.rollback()
        print(e)
        return False
def reservation(pList,qtyList, cEmail):
    argList=list(zip(pList, qtyList))
    reservationID=createReservation(argList[0][0],argList[0][1])
    if reservationID==False:
        print("Initial reservation failed")
        return False
    for x in argList[1:]:
        pName=x[0]
        qty=x[1]
        try:
            sql= "UPDATE Product Set availableStock=AvailableStock-%s where pName=%s"
            data= (qty,pName)
            ret=cursor.execute(sql, data)
            sql= "UPDATE Product Set reservedCopies=reservedCopies+%s where pName=%s"
            data= (qty,pName)
            ret=cursor.execute(sql, data)
            sql= "INSERT INTO gameReservation(reservationID,pName,copiesSold)Values (%s,%s,%s)"
            data=(reservationID,pName,qty)
            ret=cursor.execute(sql, data)
            connection.commit()
        except Exception as e:
            connection.rollback()
            print("Reservation errored for "+x)
            print(e)
            return False
    return True
def clearReservation(reservationID):
    try:
        sql= "SELECT pName,copiesReserved FROM gameReservation Where reservationID=%s"
        data= (reservationID,)
        ret=cursor.execute(sql, data)
        reservs=cursor.fetchall()
        for reserv in reservs:
            qty=reserv[1]
            pName=reserv[0]
            sql= "UPDATE Product Set availableStock=AvailableStock+%s where pName=%s"
            data= (qty,pName)
            ret=cursor.execute(sql, data)
            sql= "UPDATE Product Set reservedCopies=reservedCopies-%s where pName=%s"
            data= (qty,pName)
            ret=cursor.execute(sql, data)
        sql= "Delete FROM gameReservation Where reservationID=%s"
        data= (reservationID,)
        cursor.execute(sql, data)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print("Error")
        print(e)
        return False

def cReport(sort,asc,limit):
    try:
        sql= "SELECT distinct customers.cemail,gameReservation.pname, gameReservation.copiesReserved,gameSales.pname, gameSales.copiessold from customers"
        sql+= " left join reservationList on customers.cemail=reservationlist.cemail"
        sql+= " left join gameReservation on reservationList.reservationid=gameReservation.reservationid"
        sql+= " left join salesList on customers.cemail=salesList.cemail"
        sql+= " left join gameSales on reservationList.reservationid=gameSales.saleid"
        if sort=="reservation":
            sql+= " Order By gameReservation.copiesReserved "
        elif sort=="sales":
            sql+= " Order By gameSales.copiessold "
        else:
            sql+= " Order By customers.cemail "
        if asc:
            sql+= "ASC "
        else:
            sql+="DESC "
        sql+="LIMIT %s"
        data=(limit,)
        cursor.execute(sql, data)
        for x in cursor.fetchall():
            print(x)
    except Exception as e:
        connection.rollback()
        print("Error")
        print(e)
        return False


# Establish a connection
fail=False
user=input("Username: ")
password=input("Password: ")
host=input("Hostname(leave blank for local): ")
if host=="":
    host="localhost"
query=input("Would you like to \"login\" or \"create\" a database?").lower()
try:
    if query=="create":
        #query=input("Enter Database Name").lower()
        conn_params={
            "user" : user,
            "password" : password,
            "host" : host
        }
        connection= mariadb.connect(**conn_params)
        cursor= connection.cursor()
        sql="CREATE DATABASE vendorDatabase"
        cursor.execute(sql,)
        connection.commit()
        sql="use vendorDatabase"
        cursor.execute(sql,)
        createTables()
    else:
        query=input("Enter existing Database Name(or leave blank for default: vendorDatabase)").lower()
        if query=="":
            query="vendorDatabase"
        conn_params={
            "user" : user,
            "password" : password,
            "host" : host,
            "database" : query
        }
        connection= mariadb.connect(**conn_params)
        cursor= connection.cursor()
except Exception as e:
    print("Connection failed: invalid inputs or server status, error message: ")
    print(e)
    connection.rollback()
    cursor.close()
    connection.close()
    fail=True

def askFor(inlist):
    ret=[]
    for x in inlist:
        ret.append(input("Enter the "+x+":    "))
    return ret
def multiAskFor(x):
    ret=[]
    query=input("Enter your "+x+" or leave blank to finish listing:    ")
    while query!="":
        ret.append(query)
        query=input("Enter your "+x+" or leave blank to finish listing:    ")
    if len(ret)<1:
        print("You must have at least 1 item")
        return multiAskFor(x)
    return ret

if not fail:
    query=input("Enter a command, type \"help\" for a list of commands, or type \"exit\" to end\n")
    while query.lower()!="exit" or query.lower()!="end":
        case=query.lower()
        if case=="create product" or case=="product create":
            vals=askFor(["pname","stock", "price","copies reserved"])
            if productCreate(*vals):
                print("operation successful")
        elif case=="stock product" or case=="product stock":
            vals=askFor(["pname","quantity to stock"])
            if productStock(*vals):
                print("operation successful")
        elif case=="set product stock" or case=="product set stock":
            vals=askFor(["pname","quantity stocked"])
            if productSetStock(*vals):
                print("operation successful")
        elif case=="product change" or case=="change product":
            vals=askFor(["old Product Name","new Product Name(blank if same), new Price(blank if same)"])
            if val[1]=="":
                val[1]=val[0]
            if val[2]=="":
                if productChange(vals[0],vals[1]):
                    print("operation successful")
            else:
                if productChange(*vals):
                    print("operation successful")
        elif case=="refund":
            vals=askFor(["pname","quantity stocked"])
            if productSetStock(*vals):
                print("operation successful")
        elif case=="create employee" or case=="employee":
            vals=askFor(["employee ID","First Name","Last Name","Phone Number"])
            if createEmployee(*vals):
                print("operation successful")
        elif case=="create customer" or case=="customer":
            vals=askFor(["Customer Email","First Name","Last Name"])
            if createCustomer(*vals):
                print("operation successful")
        elif case=="create sale" or case=="sale":
            arg1=multiAskFor("product names")
            arg2=multiAskFor("product quantities")
            arg3, arg4=askFor(["customer email","employee ID"])
            if sale(arg1,arg2,arg3,arg4):
                print("operation successful")
        elif case=="create reservation" or case=="reservation":
            arg1=multiAskFor("product names")
            arg2=multiAskFor("product quantities")
            arg3=askFor(["customer email"])[0]
            if resrvation(arg1,arg2,arg3):
                print("operation successful")
        elif case=="clear reservation" or case=="remove reservation":
            arg3=askFor(["reservationID"])[0]
            if clearReservation(arg3):
                print("operation successful")
        elif case=="report" or "chart":
            argV=askFor(["sorting option(reservation, sales, or default to customer email)","ascending? True or False", "Maximum number of entries"])
            if argV[1].lower()=="true":
                argV[1]=True
            else:
                argV[1]=False
            argV[2]=int(argV[2])
            cReport(*argV)
        elif case=="help":
            print("Command List: ")
        elif case=="reformat" or case=="reset":
            if conn_params["user"]!="root":
                print("Permission denied, must be signed in as root.")
            else:
                query=input("Are you sure? this will reset the entire database. To continue type \"confirm\"").lower()
                if query=="confirm":
                    if clearDatabase() and createTables():
                        print("Operation Successful")
                    else:
                        print("operation failed")

        elif query!="exit":
            print("Command unknown")
        query=input("Enter a command, type \"help\" for a list of commands, or type \"exit\" to end\n")
    connection.commit()
    cursor.close()
    connection.close()
    exit()
