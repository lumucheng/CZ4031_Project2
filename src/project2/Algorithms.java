/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

package project2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;

public class Algorithms {

	private static int count = 0;
	
	/**
	 * Sort the relation using Setting.memorySize buffers of memory
	 * @param rel is the relation to be sorted.
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int mergeSortRelation(Relation rel) {
		int numIO = 0;
		int M = Setting.memorySize;
	
		if (Math.pow(M, 2) < rel.getNumBlocks()) {
			System.out.println("Size of memory not enough for MergeSort.");
			return 0;
		}
		
		// Produce sorted sublist
		Pair<Integer, List<Relation>> ret = sortedSubList(rel);
		numIO += ret.getLeft();
		List<Relation> sublists = ret.getRight();

		// Phase 2: Merge the sorted sublist
		int sublistSize = sublists.size();  
		List<Block> mainMemoryBuffers = new ArrayList<Block>(M);
		
		for (int i = 0; i < M-1; i++) {
			mainMemoryBuffers.add(null);
		}
		
		// Add Output Buffer at last index
		mainMemoryBuffers.add(new Block());
		
		Relation newRel = new Relation("NewRel");
		RelationWriter newRelWriter = newRel.getRelationWriter();
		
		ArrayList<RelationLoader> loaderList = new ArrayList<Relation.RelationLoader>();
		
		for (int i = 0; i < sublists.size(); i++) {
			RelationLoader loader = sublists.get(i).getRelationLoader();
			loaderList.add(i, loader);
		}
		
		int tupleCount = rel.getNumTuples();
		
		while (tupleCount > 0) {
			
			// Always check if input buffer needs to be replenished
			for (int memoryIndex = 0; memoryIndex < M-1; memoryIndex++) {
				if (mainMemoryBuffers.get(memoryIndex) == null) {
					if (memoryIndex < loaderList.size()) {
						RelationLoader loader = loaderList.get(memoryIndex);
						
						if (loader.hasNextBlock()) {
							Block block = loader.loadNextBlocks(1)[0];
							mainMemoryBuffers.set(memoryIndex, block);
							numIO++;
						}
					}
				}
			}
			
			int minKey = -1;
			int position = -1;
			for (int memoryIndex = 0; memoryIndex < M-1; memoryIndex++) {
				
				if (mainMemoryBuffers.get(memoryIndex) != null && 
						(mainMemoryBuffers.get(memoryIndex).tupleLst.get(0).key <= minKey || minKey == -1)) {
					minKey = mainMemoryBuffers.get(memoryIndex).tupleLst.get(0).key;
					position = memoryIndex;
				}
			}
			
			if (position > -1) {
				Block minBlock = mainMemoryBuffers.get(position);
				if (minBlock != null) {
					Tuple minTuple = minBlock.tupleLst.get(0);
					minBlock.tupleLst.remove(0);
					
					if (minBlock.getNumTuples() == 0) {
						mainMemoryBuffers.set(position, null);
					}
					
					mainMemoryBuffers.get(M-1).tupleLst.add(minTuple);
					if (mainMemoryBuffers.get(M-1).getNumTuples() == Setting.blockFactor) {
						newRelWriter.writeBlock(mainMemoryBuffers.get(M-1));
						tupleCount -= Setting.blockFactor;
						mainMemoryBuffers.set(M-1, new Block());
					}
				}
			}
			else { // Last Block
				Block lastBlock = mainMemoryBuffers.get(M-1);
				newRelWriter.writeBlock(lastBlock);
				tupleCount -= lastBlock.getNumTuples();
			}
		}
		
		newRel.printRelation(true, true);
		return numIO;
	}

	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory
	 * to produce the result relation relRS
	 * 
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int hashJoinRelations(Relation relR, Relation relS, Relation relRS) {
		int numIO = 0;
		int M = Setting.memorySize;
		Block[] mainMemoryBuffers = new Block[M];
		ArrayList<ArrayList<Block>> rBuckets = new ArrayList<ArrayList<Block>>(M - 1);
		ArrayList<ArrayList<Block>> sBuckets = new ArrayList<ArrayList<Block>>(M - 1);
		ArrayList<JointTuple> jointTupleList = new ArrayList<JointTuple>();
		
		if (Math.min(relR.getNumBlocks(), relR.getNumBlocks()) > M * M) {
			System.out.println("Not enough memory for Hash Join. Try to increase memory.");
			return 0;
		}
		
		// Initialize M-1 Memory Buffers. Last index of buffer will be used as input buffer
		for (int i = 0; i < M - 1; i++) {
			mainMemoryBuffers[i] = new Block();
		}

		// Initialize M-1 buckets for relation R & S
		// This will simulate the buckets that will be found on disk
		for (int i = 0; i < M - 1; i++) {
			ArrayList<Block> rBucket = new ArrayList<Block>(M-1);
			rBuckets.add(rBucket);
			ArrayList<Block> sBucket = new ArrayList<Block>(M-1);
			sBuckets.add(sBucket);
		}

		// Hash Relation R
		RelationLoader rLoader = relR.getRelationLoader();
		while (rLoader.hasNextBlock()) {
			Block[] blocks = rLoader.loadNextBlocks(1);
			numIO++;

			// read block b into Mth buffer
			mainMemoryBuffers[M-1] = blocks[0];
			ArrayList<Tuple> tupleList = (ArrayList<Tuple>)mainMemoryBuffers[M-1].tupleLst.clone();

			for (Tuple tuple : tupleList) {

				int bucketToHashTo = tuple.key % (M - 1);
				
				if (mainMemoryBuffers[bucketToHashTo].getNumTuples() == Setting.blockFactor) {

					Block fullBuffer = mainMemoryBuffers[bucketToHashTo];
					ArrayList<Block> hashBucket = rBuckets.get(bucketToHashTo);

					// Copy buffer to disk
					hashBucket.add(fullBuffer);
					numIO++;

					// Initialize new empty block in buffer
					mainMemoryBuffers[bucketToHashTo] = new Block();
				}

				mainMemoryBuffers[bucketToHashTo].insertTuple(tuple);
			}
		}

		for (int i = 0; i < M - 1; i++) {
			if (mainMemoryBuffers[i].getNumTuples() > 0) {
				ArrayList<Block> hashBucket = rBuckets.get(i);
				hashBucket.add(mainMemoryBuffers[i]);
				numIO++;
			}
			
			// Reset memory buffer for S use later
			mainMemoryBuffers[i] = new Block();
		}

		for (int i = 0; i < rBuckets.size(); i++) {			
			ArrayList<Block> blocks = rBuckets.get(i);
			System.out.println("R Bucket " + i + " has size " + blocks.size());
		}
		
		// Hash Relation S
		RelationLoader sLoader = relS.getRelationLoader();
		while (sLoader.hasNextBlock()) {
			Block[] blocks = sLoader.loadNextBlocks(1);
			numIO++;

			// read block b into Mth buffer
			mainMemoryBuffers[M-1] = blocks[0];
			ArrayList<Tuple> tupleList = (ArrayList<Tuple>)mainMemoryBuffers[M-1].tupleLst.clone();

			for (Tuple tuple : tupleList) {

				int bucketToHashTo = tuple.key % (M - 1);

				if (mainMemoryBuffers[bucketToHashTo].getNumTuples() == Setting.blockFactor) {

					Block fullBuffer = mainMemoryBuffers[bucketToHashTo];
					ArrayList<Block> hashBucket = sBuckets.get(bucketToHashTo);

					// Copy buffer to disk
					hashBucket.add(fullBuffer);
					numIO++;

					// Initialize new empty block in buffer
					mainMemoryBuffers[bucketToHashTo] = new Block();
				}

				mainMemoryBuffers[bucketToHashTo].insertTuple(tuple);
			}
		}
				
		for (int i = 0; i < M - 1; i++) {
			if (mainMemoryBuffers[i].getNumTuples() > 0) {
				ArrayList<Block> hashBucket = sBuckets.get(i);
				hashBucket.add(mainMemoryBuffers[i]);
				numIO++;
			}
			
			// Reset memory buffer for Hash Join use later
			mainMemoryBuffers[i] = null;
		}
		
		for (int i = 0; i < sBuckets.size(); i++) {
			ArrayList<Block> blocks = sBuckets.get(i);
			System.out.println("S Bucket " + i + " has size " + blocks.size());
		}
		
		// Hash Join
		for (int i = 0; i < sBuckets.size(); i++) {
			
			ArrayList<Block> sBucket = sBuckets.get(i);
			ArrayList<Block> rBucket = rBuckets.get(i);
			
			// Read in blocks from one bucket of S
			for (int x = 0; x < sBucket.size(); x++) {
				mainMemoryBuffers[x] = sBucket.get(x);
				numIO++;
			}
			
			// Read in one block from one bucket of R
			for (Block rBlock : rBucket) {
				numIO++;
				
				for (int y = 0; y < sBucket.size(); y++) {
					
					// Compare each block
					Block sBlock = mainMemoryBuffers[y];
					
					for (int sIndex = 0; sIndex < sBlock.getNumTuples(); sIndex++) {
						for (int rIndex = 0; rIndex < rBlock.getNumTuples(); rIndex++) {
							if (sBlock.tupleLst.get(sIndex).key == rBlock.tupleLst.get(rIndex).key) {

								JointTuple jointTuple = new JointTuple(rBlock.tupleLst.get(rIndex), 
										sBlock.tupleLst.get(sIndex));
								jointTupleList.add(jointTuple);
							}
						}
					}
				}
			}
		}
		
		RelationWriter rsWriter = relRS.getRelationWriter();
		Block block = new Block();
		
		for (JointTuple jointTuple : jointTupleList) {
			block.tupleLst.add(jointTuple);
			
			if (block.getNumTuples() == Setting.blockFactor) {
				rsWriter.writeBlock(block);
				block = new Block();
			}
		}
		
		// Last block
		if (block.getNumTuples() > 0) {
			rsWriter.writeBlock(block);
		}
		
		relRS.printRelation(false, false);
		return numIO;
	}
	
	static class Pair<Left, Right> {

        private final Left left;
        private final Right right;

        public Pair(Left left, Right right) {
            this.left = left;
            this.right = right;
        }

        public Left getLeft() {
            return left;
        }

        public Right getRight() {
            return right;
        }
    }

	public static Pair<Integer, List<Relation>> sortedSubList(Relation rel) {
        int numIO = 0;
        RelationLoader rLoader = rel.getRelationLoader();
        List<Relation> sublists = new ArrayList<Relation>();
        Relation newRel = new Relation(String.valueOf(count));
        RelationWriter rWriter = newRel.getRelationWriter();
        Block b = new Block();
        
        while (rLoader.hasNextBlock()) {
            Block[] blocks = rLoader.loadNextBlocks(Setting.memorySize);
            newRel = new Relation(String.valueOf(count));
            rWriter = newRel.getRelationWriter();
            List<Tuple> sort = new ArrayList();
            for (Block block : blocks) {
                if (block != null) {
                    for (Tuple p : block.tupleLst) {
                        sort.add(p);
                    }
                    numIO++;
                }
            }
            
            Collections.sort(sort, new Comparator<Tuple>() {
                public int compare(Tuple t1, Tuple t2) {
                    return t1.key - t2.key;
                }
            });
            
            b = new Block();
            for (int i = 0; i < sort.size(); i++) {
                if (b.getNumTuples() < Setting.blockFactor) {
                    b.insertTuple(sort.get(i));
                }
                if (b.getNumTuples() == Setting.blockFactor) {
                    rWriter.writeBlock(b);
                    b = new Block();
                    numIO++;
                }
            }
            sublists.add(newRel);
            count++;
        }
        
        if (b.getNumTuples() > 0) {
        	rWriter.writeBlock(b); //write last block 
            numIO++;
        }
        
        return new Pair<Integer, List<Relation>>(numIO, sublists);
    }

	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory
	 * to produce the result relation relRS
	 * 
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */

	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS) {
		int numIO = 0;
        int M = Setting.memorySize;
        
		if ((M * M) < (relR.getNumBlocks() + relS.getNumBlocks())) {
			System.out.println("Size of memory not enough for Refined Sort Merge Join.");
			return 0;
		}

        // Create sorted sublists of size in Setting.memorySize, using Integer as the sort key, for relR and relS
        Pair<Integer, List<Relation>> SortedRelR = sortedSubList(relR);
        numIO += SortedRelR.left;
        List<Relation> relT = SortedRelR.right;

        Pair<Integer, List<Relation>> SortedRels = sortedSubList(relS);
        numIO += SortedRels.left;
        List<Relation> relU = SortedRels.right;
        List<Block> inputBuffers = new ArrayList<Block>();
        List<JointTuple> jointTuples = new ArrayList<JointTuple>();
        List noOfBlockUsed = new ArrayList();
        
        if (M < relT.size() + relU.size()) {
        	System.out.println("Number of sublists more than M buffer. Try to increase memory size.");
        	return 0;
        }
        
        for (int i = 0; i < M - 1; i++) {
            inputBuffers.add(new Block());
        }
        int i = 0;

        for (Relation t : relT) {
            RelationLoader rLoader = t.getRelationLoader();
            inputBuffers.set(i, rLoader.loadNextBlocks(1)[0]);
            noOfBlockUsed.add(i, 0);
            i++;
            numIO++;
        }
        
        for (Relation u : relU) {
            RelationLoader rLoader = u.getRelationLoader();
            inputBuffers.set(Integer.valueOf(u.name), rLoader.loadNextBlocks(1)[0]);
            noOfBlockUsed.add(Integer.valueOf(u.name), 0);
            numIO++;
        }
        
        Pair<Integer, Tuple> pair = null;
        while (true) {
            int min = 99999;
            int a = 0;
            List<Pair> SortedPair = new ArrayList<Pair>();
            List<Pair> SortedPair1 = new ArrayList<Pair>();
            
            for (int noOfBuffer = 0; noOfBuffer < relT.size() + relU.size(); noOfBuffer++) {
                if (inputBuffers.get(noOfBuffer) != null) {
                    if (inputBuffers.get(noOfBuffer).getNumTuples() == 0) {
                        if (a < relT.size()) {

                            if (Integer.valueOf(noOfBlockUsed.get(noOfBuffer).toString()) < relT.get(noOfBuffer).getNumBlocks() - 1) {
                                inputBuffers.set(noOfBuffer, relT.get(noOfBuffer).getRelationLoader().loadNextBlocks(M)[Integer.valueOf(noOfBlockUsed.get(noOfBuffer).toString()) + 1]);
                                noOfBlockUsed.set(noOfBuffer, Integer.valueOf(noOfBlockUsed.get(noOfBuffer).toString()) + 1);
                                numIO++;
                            }
                        } 
                        else {
                            if (Integer.valueOf(noOfBlockUsed.get(noOfBuffer).toString()) < relU.get(noOfBuffer - relT.size()).getNumBlocks() - 1) {
                                int offset = noOfBuffer - relT.size();
                                inputBuffers.set(noOfBuffer, relU.get(offset).getRelationLoader().loadNextBlocks(M)[Integer.valueOf(noOfBlockUsed.get(noOfBuffer).toString()) + 1]);
                                noOfBlockUsed.set(noOfBuffer, Integer.valueOf(noOfBlockUsed.get(noOfBuffer).toString()) + 1);
                                numIO++;
                            }
                        }
                    }
                }

                if (inputBuffers.get(noOfBuffer) != null && inputBuffers.get(noOfBuffer).getNumTuples() > 0) {
                    if (min > inputBuffers.get(noOfBuffer).tupleLst.get(0).key) {
                        SortedPair = new ArrayList<Pair>();
                        min = inputBuffers.get(noOfBuffer).tupleLst.get(0).key;
                        pair = new Pair<Integer, Tuple>(a, inputBuffers.get(noOfBuffer).tupleLst.get(0));
                        SortedPair.add(pair);

                    } 
                    else if (min == inputBuffers.get(noOfBuffer).tupleLst.get(0).key) {
                        pair = new Pair<Integer, Tuple>(a, inputBuffers.get(noOfBuffer).tupleLst.get(0));
                        SortedPair.add(pair);
                    }
                    
                    int getNext = 1;
                    while (inputBuffers.get(noOfBuffer).getNumTuples() > getNext && inputBuffers.get(noOfBuffer).tupleLst.get(0 + getNext).key == min) {
                        pair = new Pair<Integer, Tuple>(a, inputBuffers.get(noOfBuffer).tupleLst.get(0 + getNext));
                        SortedPair.add(pair);
                        getNext++;
                    }
                }
                a++;
            }
            
            List<Tuple> tmpJoinR = new ArrayList<Tuple>();  // temporary list to store tuple which has the same min key
            List<Tuple> tmpJoinS = new ArrayList<Tuple>();
            if (SortedPair.size() == 0) {
                break;
            }
            if (SortedPair.size() >= 1) {
                for (Pair p : SortedPair) {
                    if (Integer.valueOf(p.left.toString()) >= relT.size()) {
                        tmpJoinR.add((Tuple) p.right);
                    } 
                    else {
                        tmpJoinS.add((Tuple) p.right);
                    }
                    
                    int blockNo = Integer.valueOf(p.left.toString());
                    inputBuffers.get(blockNo).tupleLst.remove(0);
                    if (inputBuffers.get(Integer.valueOf(p.left.toString())).getNumTuples() == 0) {
                        if (blockNo < relT.size()) {

                            if (Integer.valueOf(noOfBlockUsed.get(blockNo).toString()) < relT.get(blockNo).getNumBlocks() - 1) {
                                inputBuffers.set(blockNo, relT.get(blockNo).getRelationLoader().loadNextBlocks(M)[Integer.valueOf(noOfBlockUsed.get(blockNo).toString()) + 1]);
                                noOfBlockUsed.set(blockNo, Integer.valueOf(noOfBlockUsed.get(blockNo).toString()) + 1);
                                numIO++;
                            }
                        } 
                        else {
                            if (Integer.valueOf(noOfBlockUsed.get(blockNo).toString()) < relU.get(blockNo - relT.size()).getNumBlocks() - 1) {
                                int offset = blockNo - relT.size();
                                inputBuffers.set(blockNo, relU.get(offset).getRelationLoader().loadNextBlocks(M)[Integer.valueOf(noOfBlockUsed.get(blockNo).toString()) + 1]);
                                noOfBlockUsed.set(blockNo, Integer.valueOf(noOfBlockUsed.get(blockNo).toString()) + 1);
                                numIO++;
                            }
                        }
                        if (inputBuffers.get(blockNo).getNumTuples() > 0) {
                            if (min == inputBuffers.get(blockNo).tupleLst.get(0).key) {
                                pair = new Pair<Integer, Tuple>(blockNo, inputBuffers.get(blockNo).tupleLst.get(0));
                                SortedPair1.add(pair);
                                int getNext = 1;
                                while (inputBuffers.get(blockNo).getNumTuples() > getNext && inputBuffers.get(blockNo).tupleLst.get(0 + getNext).key == min) {

                                    pair = new Pair<Integer, Tuple>(blockNo, inputBuffers.get(blockNo).tupleLst.get(0 + getNext));
                                    SortedPair1.add(pair);

                                    getNext++;

                                }
                            }
                        }
                    }
                }
            }
            
            if (SortedPair1.size() >= 1) {
                for (Pair p : SortedPair1) {
                    if (Integer.valueOf(p.left.toString()) >= relT.size()) {
                        tmpJoinR.add((Tuple) p.right);
                    } 
                    else {
                        tmpJoinS.add((Tuple) p.right);
                    }

                    int blockNo = Integer.valueOf(p.left.toString());
                    inputBuffers.get(blockNo).tupleLst.remove(0);
                }
            }

            for (Tuple tr : tmpJoinR) {
                for (Tuple ts : tmpJoinS) {
                    jointTuples.add(new JointTuple(tr, ts));
                }
            }
        }
        
        RelationWriter rsWriter = relRS.getRelationWriter();
        Block rsBlock = new Block();
        for (JointTuple jointTuple : jointTuples) {
            rsBlock.insertTuple(jointTuple);

            if (rsBlock.getNumTuples() == Setting.blockFactor) {
                rsWriter.writeBlock(rsBlock);
                rsBlock = new Block();
            }
        }
        
        if (rsBlock.getNumTuples() > 0) {
        	rsWriter.writeBlock(rsBlock); //last block
        }
        
        relRS.printRelation(true, true);
        return numIO;
	}

	/**
	 * Example usage of classes.
	 */
	public static void examples() {

		/* Populate relations */
		System.out.println("---------Populating two relations----------");
		Relation relR = new Relation("RelR");
		int numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		Relation relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out
				.println("---------Finish populating relations----------\n\n");

		/* Print the relation */
		System.out.println("---------Printing relations----------");
		relR.printRelation(true, true);
		relS.printRelation(true, false);
		System.out.println("---------Finish printing relations----------\n\n");

		/* Example use of RelationLoader */
		System.out
				.println("---------Loading relation RelR using RelationLoader----------");
		RelationLoader rLoader = relR.getRelationLoader();
		while (rLoader.hasNextBlock()) {
			System.out
					.println("--->Load at most 7 blocks each time into memory...");
			Block[] blocks = rLoader.loadNextBlocks(7);
			// print out loaded blocks
			for (Block b : blocks) {
				if (b != null)
					b.print(false);
			}
		}
		System.out.println("---------Finish loading relation RelR----------\n\n");

		/* Example use of RelationWriter */
		System.out.println("---------Writing to relation RelS----------");
		RelationWriter sWriter = relS.getRelationWriter();
		rLoader.reset();
		if (rLoader.hasNextBlock()) {
			System.out.println("Writing the first 7 blocks from RelR to RelS");
			System.out.println("--------Before writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);

			Block[] blocks = rLoader.loadNextBlocks(7);
			for (Block b : blocks) {
				if (b != null)
					sWriter.writeBlock(b);
			}
			System.out.println("--------After writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
		}

	}

	/**
	 * Testing cases.
	 */
	public static void testCases() {

		// Insert your test cases here!
		
		Algorithms algo = new Algorithms();
		
		Relation relR = new Relation("RelR");
		int numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		System.out.println("Relation RelR contains " + relR.getNumBlocks() + " blocks."); 
		Relation relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("Relation RelS contains " + relS.getNumBlocks() + " blocks.");
		System.out.println("---------Finish populating relations----------\n");
		
//		int numIO = algo.mergeSortRelation(relR);
//		System.out.println("Number of Disks I/O for MergeSort: " + numIO);
//		int numIO = algo.mergeSortRelation(relS);
//		System.out.println("Number of Disks I/O for MergeSort: " + numIO);
		
		Relation relRS = new Relation("RelRS");
		int numIO = algo.hashJoinRelations(relR, relS, relRS);
		System.out.println("Number of Disks I/O for Hash Join: " + numIO);
		
//		Relation relRS = new Relation("RelRS");
//		int numIO = algo.refinedSortMergeJoinRelations(relR, relS, relRS);
//		System.out.println("Number of Disks I/O for Refined Sort Merge Join: " + numIO);
	}

	/**
	 * This main method provided for testing purpose
	 * 
	 * @param arg
	 */
	public static void main(String[] arg) {
		// Algorithms.examples();
		
		testCases();
	}
}
