package io.narayana;

import oracle.jdbc.xa.OracleXAException;
import org.apache.commons.lang3.RandomUtils;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleTest {
    public static void main(String[] args) {
        try {
            oracle.jdbc.xa.client.OracleXADataSource ds = new oracle.jdbc.xa.client.OracleXADataSource();
            ds.setURL("jdbc:oracle:thin:@127.0.0.1:1521:xe");
            ds.setUser("sa");
            ds.setPassword("sa");


            XAConnection conn1 = ds.getXAConnection();
            XAResource res1 = conn1.getXAResource();
            Xid xid = new Xid() {
                private int formatId = 0;
                private byte[] gtrid = RandomUtils.nextBytes(4);
                private byte[] bqual = RandomUtils.nextBytes(4);

                @Override
                public int getFormatId() {
                    return formatId;
                }

                @Override
                public byte[] getGlobalTransactionId() {
                    return gtrid;
                }

                @Override
                public byte[] getBranchQualifier() {
                    return bqual;
                }

                @Override
                public String toString() {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("< ");
                    stringBuilder.append(formatId);
                    stringBuilder.append(", ");

                    for (int i = 0; i < gtrid.length; i++) {
                        stringBuilder.append(gtrid[i]);
                    }
                    stringBuilder.append(", ");
                    for (int i = 0; i < bqual.length; i++) {
                        stringBuilder.append(bqual[i]);
                    }

                    stringBuilder.append(" >");
                    return stringBuilder.toString();
                }

            };

            boolean result = res1.setTransactionTimeout(60 * 60 * 24 * 365);
            System.out.println("set transaction timeout " + result);
            System.out.println("res1 current timeout is " + res1.getTransactionTimeout());

            System.out.println("res1 start xid " + xid);
            res1.start(xid, XAResource.TMNOFLAGS);
            Statement stmt = conn1.getConnection().createStatement();
            stmt.executeUpdate("insert into kvpair values ('a', 'a')");
            res1.end(xid, XAResource.TMSUCCESS);
            System.out.println("res1 end xid " + xid);

            int r = res1.prepare(xid);
            System.out.println("res1 prepare xid " + xid + " with result " + r);

            XAConnection conn2 = ds.getXAConnection();
            XAResource res2 = conn2.getXAResource();
            System.out.println("res2 current timeout is " + res2.getTransactionTimeout());

            Xid[] xids = res2.recover(XAResource.TMSTARTRSCAN);
            System.out.println("res2 get " + xids.length + " xid to recover");

            res2.rollback(xids[0]);
            System.out.println("res2 rollback xid ok");

            res2.recover(XAResource.TMENDRSCAN);
        }
        catch (SQLException e) {
            System.err.println("Oracle XADataSource fails with " + e);
            e.printStackTrace();
        } catch (OracleXAException e) {
            System.err.println("XAResource fails with oracle error " + e.getOracleError());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}