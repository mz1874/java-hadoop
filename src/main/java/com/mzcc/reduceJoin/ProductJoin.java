package com.mzcc.reduceJoin;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class ProductJoin implements Writable {

    private String id;

    private String name;

    private String pid;

    private String quantity;

    private String flag;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(null == name ? "" : name);
        out.writeUTF(null == quantity? "" : quantity);
        out.writeUTF(flag);
        out.writeUTF(pid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.quantity = in.readUTF();
        this.flag = in.readUTF();
        this.pid = in.readUTF();
    }

    @Override
    public String toString() {
        return this.id + "\t" + this.pid + "\t" + this.name + "\t" + this.quantity + "\t" + this.flag;
    }
}
