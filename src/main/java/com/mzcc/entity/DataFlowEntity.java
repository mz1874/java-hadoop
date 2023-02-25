package com.mzcc.entity;

import lombok.Data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author aiden
 * @data 19/02/2023
 * @description
 */
@Data
public class DataFlowEntity implements Writable {

    private Text phoneNumber;

    private int upFlowNumber;

    private int downFlowNumber;

    private Text dateMonth;

    private int totalDateFlowNumber;

    public int getTotalDateFlowNumber() {
        return totalDateFlowNumber;
    }

    public void setTotalDateFlowNumber() {
        this.totalDateFlowNumber = upFlowNumber + downFlowNumber;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlowNumber);
        dataOutput.writeInt(downFlowNumber);
        dataOutput.writeInt(totalDateFlowNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlowNumber=dataInput.readInt();
        this.downFlowNumber=dataInput.readInt();
        this.totalDateFlowNumber=dataInput.readInt();
    }



}
