
public class CountDriver {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String forTo[] ={args[0],args[1]};
		ToUndirectedGraph.main(forTo);
		String forCN[] ={args[1],args[2]};
		CountNum.main(forCN);

	}

}
