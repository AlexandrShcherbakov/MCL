public class IdAndWeightWritable implements WritableComparable {
	private int id;
	private float weight;

	public IdAndWeightWritable() {
	}

	public IdAndWeightWritable(int id, float weight) {
		this.id = id;
		this.weight = weight;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeFloat(weight);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		weight = in.readFloat();
	}

	public int compareTo(IdAndWeightWritable o) {
		return id < o.id ? -1 : (id == o.id ? 0 : 1);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + (int)(1 / weight);
		return result;
	}

	public int getId() {
		return id;
	}

	public float getWeight() {
		return weight;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	public void set(int id, float weight) {
		this.id = id;
		this.weight = weight;
	}

	public String toString() {
		return Integer.toString(id) + ":" + Float.toString(weight);
	}
}