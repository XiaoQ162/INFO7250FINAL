package WritableComparables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OriginDestMonth implements WritableComparable {
    private String origin;
    private String destination;
    private String month;

    public OriginDestMonth() {
        super();
    }

    public OriginDestMonth(String origin, String destination, String month) {
        super();
        this.origin = origin;
        this.destination = destination;
        this.month = month;
    }

    @Override
    public String toString() {
        return origin + " " + destination + " " + month;
    }

    @Override
    public int compareTo(Object o) {
        if (o.getClass() != this.getClass()) {
            return 0;
        } else {
            int result = this.origin.compareTo(((OriginDestMonth)o).getOrigin());
            if (result != 0) {
                return result;
            }
            result = this.destination.compareTo(((OriginDestMonth)o).getDestination());
            if (result != 0) {
                return result;
            }
            result = this.month.compareTo(((OriginDestMonth)o).getMonth());
            if (result != 0) {
                return result;
            }
            return  0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(origin);
        dataOutput.writeUTF(destination);
        dataOutput.writeUTF(month);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        origin =dataInput.readUTF();
        destination = dataInput.readUTF();
        month = dataInput.readUTF();
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }
}
