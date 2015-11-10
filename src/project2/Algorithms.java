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
	 * Sort the relation using Setting.memorySize buffers of memory
	 * @param rel is the relation to be sorted.
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int mergeSortRelation(Relation rel) {
		int numIO = 0;
		int M = Setting.memorySize;
		count = 0;
	
		// Check if enough memory buffers
		if (M * M < rel.getNumBlocks()) {
			System.out.println("Size of memory not enough for MergeSort.");
			return 0;
		}
		
		// ---------------------------------
		// Phase 1: Produce sorted sublist
		
		Pair<Integer, List<Relation>> ret = sortedSubList(rel);
		numIO += ret.getLeft();
		List<Relation> sortedSubLists = ret.getRight();
		
		if (sortedSubLists.size() > M-1) {
			System.out.println("There are more sorted sublists than M-1, unable to do merge sort.");
			return 0;
		}
			
		int min = sortedSubLists.get(0).getNumBlocks();
		int max = sortedSubLists.get(0).getNumBlocks();
		int total = 0;
		
		for (Relation sublist : sortedSubLists) {
			
			total += sublist.getNumBlocks();
			
			if (sublist.getNumBlocks() < min) {
				min = sublist.getNumBlocks();
			}
			
			if (sublist.getNumBlocks() > max) {
				max = sublist.getNumBlocks();
			}
		}
		double avg = (double)total / (double)sortedSubLists.size(); 
		
		// ---------------------------------
		// Phase 2: Merge the sorted sublist
		
		// Create and instantiate memory buffers
		List<Block> mainMemoryBuffers = new ArrayList<Block>(M);
		for (int i = 0; i < M - 1; i++) {
			mainMemoryBuffers.add(null);
		}
		
		// Add Output Buffer at last index
		mainMemoryBuffers.add(new Block());
		
		Relation newRel = new Relation("Sorted" + rel.name);
		RelationWriter newRelWriter = newRel.getRelationWriter();
		
		// Create a list of relation loader that will hold loader references
		// for replenishing input buffers later
		ArrayList<RelationLoader> loaderList = new ArrayList<Relation.RelationLoader>();
		for (int i = 0; i < sortedSubLists.size(); i++) {
			RelationLoader loader = sortedSubLists.get(i).getRelationLoader();
			loaderList.add(i, loader);
		}
		
		int tupleCount = rel.getNumTuples();
		while (tupleCount > 0) {
			
			// Always check if input buffer needs to be replenished
			for (int memoryIndex = 0; memoryIndex < M - 1; memoryIndex++) {
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
			
			// Iterate through to get the smallest key & the corresponding memory index from the input buffers
			for (int memoryIndex = 0; memoryIndex < M-1; memoryIndex++) {
				if (mainMemoryBuffers.get(memoryIndex) != null && 
						(minKey == -1 || mainMemoryBuffers.get(memoryIndex).tupleLst.get(0).key <= minKey)) {
					minKey = mainMemoryBuffers.get(memoryIndex).tupleLst.get(0).key;
					position = memoryIndex;
				}
			}
			
			// if position equals -1, implies that all blocks have been read and input 
			// buffers are empty. Proceed to write what's left in output buffer to disk
			if (position != -1) {
				
				// Get block that has the smallest key from input buffers
				Block minBlock = mainMemoryBuffers.get(position);
				
				// Get the tuple with smallest key and remove it from the block
				Tuple minTuple = minBlock.tupleLst.get(0);
				minBlock.tupleLst.remove(0);

				// No more tuples in block, set it to null for replenish later
				if (minBlock.getNumTuples() == 0) {
					
					mainMemoryBuffers.set(position, null);
				}

				// Add tuple to output buffer
				mainMemoryBuffers.get(M-1).tupleLst.add(minTuple);
				
				// Write output buffer to disk if full
				if (mainMemoryBuffers.get(M-1).getNumTuples() == Setting.blockFactor) {
					newRelWriter.writeBlock(mainMemoryBuffers.get(M-1));
					
					// Decrease tuple count in order to eventually terminate while loop
					tupleCount -= Setting.blockFactor;
					
					// Reset output buffer
					mainMemoryBuffers.set(M-1, new Block());
				}
			}
			else { // Last Block
				Block lastBlock = mainMemoryBuffers.get(M-1);
				newRelWriter.writeBlock(lastBlock);
				
				// Decrease tuple count in order to terminate while loop
				tupleCount -= lastBlock.getNumTuples();
			}
		}
		
		newRel.printRelation(false, false);
		System.out.println("Number of sublists generated: " + sortedSubLists.size());
		System.out.println("Maximum length of sublist: " + max);
		System.out.println("Minimum length of sublist: " + min);
		System.out.println("Average length of sublists: " + avg);		
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
			System.out.println("Size of memory not enough for Hash Join.");
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

		// Step 1: Hash Relation R
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
					
					if (hashBucket.size() > M - 1) {
						System.out.println("Bucket size exceeded M-1 for Relation " + relR.name + 
								" at hash " + bucketToHashTo + ", unable to do hash join.");
						return 0;
					}
					
					// Initialize new empty block in buffer
					mainMemoryBuffers[bucketToHashTo] = new Block();
				}

				mainMemoryBuffers[bucketToHashTo].insertTuple(tuple);
			}
		}

		// For each buffer, copy to disk if not empty
		for (int i = 0; i < M - 1; i++) {
			if (mainMemoryBuffers[i].getNumTuples() > 0) {
				ArrayList<Block> hashBucket = rBuckets.get(i);
				hashBucket.add(mainMemoryBuffers[i]);
				numIO++;
			}
			
			// Reset memory buffer for S use later
			mainMemoryBuffers[i] = new Block();
		}
		
//		DELETE LATER		
//
//		for (int i = 0; i < rBuckets.size(); i++) {			
//			ArrayList<Block> blocks = rBuckets.get(i);
//			System.out.println("R Bucket " + i + " has size " + blocks.size());
//		}
//		
		// Step 1: Hash Relation S
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
					
					if (hashBucket.size() > M - 1) {
						System.out.println("Bucket size exceeded M-1 for Relation " + relS.name + 
								" at hash " + bucketToHashTo + ", unable to do hash join.");
						return 0;
					}

					// Initialize new empty block in buffer
					mainMemoryBuffers[bucketToHashTo] = new Block();
				}

				mainMemoryBuffers[bucketToHashTo].insertTuple(tuple);
			}
		}

		// For each buffer, copy to disk if not empty
		for (int i = 0; i < M - 1; i++) {
			if (mainMemoryBuffers[i].getNumTuples() > 0) {
				ArrayList<Block> hashBucket = sBuckets.get(i);
				hashBucket.add(mainMemoryBuffers[i]);
				numIO++;
			}
			
			// Reset memory buffer for Hash Join use later
			mainMemoryBuffers[i] = null;
		}

		// DELETE LATER
//		for (int i = 0; i < sBuckets.size(); i++) {
//			ArrayList<Block> blocks = sBuckets.get(i);
//			System.out.println("S Bucket " + i + " has size " + blocks.size());
//		}
		
		// Step 2: Perform Join
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
		
		
		// Write join result to disk
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
        count = 0;
        
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
        List<Integer> noOfBlockUsed = new ArrayList<Integer>();
        
        if (M * M < relT.size() + relU.size()) {
        	System.out.println("Number of sublists more than M buffer. Try to increase memory size.");
        	return 0;
        }
        
		int rMin = relT.get(0).getNumBlocks();
		int rMax = relT.get(0).getNumBlocks();
		int rTotal = 0;
		
		for (Relation sublist : relT) {
			
			rTotal += sublist.getNumBlocks();
			
			if (sublist.getNumBlocks() < rMin) {
				rMin = sublist.getNumBlocks();
			}
			
			if (sublist.getNumBlocks() > rMax) {
				rMax = sublist.getNumBlocks();
			}
		}
		double rAvg = (double)rTotal / (double)relT.size();
		
		int sMin = relU.get(0).getNumBlocks();
		int sMax = relU.get(0).getNumBlocks();
		int sTotal = 0;
		
		for (Relation sublist : relU) {
			
			sTotal += sublist.getNumBlocks();
			
			if (sublist.getNumBlocks() < sMin) {
				sMin = sublist.getNumBlocks();
			}
			
			if (sublist.getNumBlocks() > sMax) {
				sMax = sublist.getNumBlocks();
			}
		}
		double sAvg = (double)sTotal / (double)relU.size(); 
        
        
        for (int i = 0; i < M - 1; i++) {
            inputBuffers.add(new Block());
        }
        int i = 0;
        
        if (relU.size() + relT.size() > M - 1) {
        	System.out.println("There are more than M sublists, unable to do refined sort merge join.");
        	return 0;
        }

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
        
        relRS.printRelation(false, false);
		System.out.println("Number of sublists generated for R: " + relT.size());
		System.out.println("Maximum length of R sublist: " + rMax);
		System.out.println("Minimum length of R sublist: " + rMin);
		System.out.println("Average length of R sublists: " + rAvg);
		System.out.println("Number of sublists generated for S: " + relU.size());
		System.out.println("Maximum length of S sublist: " + sMax);
		System.out.println("Minimum length of S sublist: " + sMin);
		System.out.println("Average length of S sublists: " + sAvg);
        
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
		System.out.println("---------Finish populating relations----------\n\n");

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

		Relation relR;
		Relation relS;
		Relation relRS;
		
		int numTuples = 0;
		int mergeSortRIO = 0;
		int mergeSortSIO = 0;
		int hashJoinIO = 0;
		int refinedSortMergeJoinIO = 0;
		
		Algorithms algo = new Algorithms();
		
		System.out.println("(Test Case 1)");
		System.out.println("The block factor is " + Setting.blockFactor);
		System.out.println("The memory size is " + Setting.memorySize);
		
		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		System.out.println("Relation RelR contains " + relR.getNumBlocks() + " blocks."); 
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("Relation RelS contains " + relS.getNumBlocks() + " blocks.");
		System.out.println("---------Finish populating relations----------\n");
		
		System.out.println("[MergeSort Algorithm]");
		mergeSortRIO = algo.mergeSortRelation(relR);
		System.out.println("Number of Disks I/O for MergeSort on " + relR.name + ": " + mergeSortRIO + "\n");
		mergeSortSIO = algo.mergeSortRelation(relS);
		System.out.println("Number of Disks I/O for MergeSort on " + relS.name + ": " + mergeSortSIO + "\n");
		
		System.out.println("[Hash Join Algorithm]");
		relRS = new Relation("RelRS");
		hashJoinIO = algo.hashJoinRelations(relR, relS, relRS);
		System.out.println("Number of Disks I/O for Hash Join: " + hashJoinIO + "\n");
		
		System.out.println("[Refined Sort Merge Join Algorithm]");
		relRS = new Relation("RelRS");
		refinedSortMergeJoinIO = algo.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("Number of Disks I/O for Refined Sort Merge Join: " + refinedSortMergeJoinIO + "\n");
		System.out.println("------------------------------------------------------------");
		// ------------------------------------------------------------------------------------
		
		System.out.println("(Test Case 2)");
		Setting.blockFactor = 20;
		Setting.memorySize = 10;
		System.out.println("The block factor is " + Setting.blockFactor);
		System.out.println("The memory size is " + Setting.memorySize);
		
		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		System.out.println("Relation RelR contains " + relR.getNumBlocks() + " blocks."); 
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("Relation RelS contains " + relS.getNumBlocks() + " blocks.");
		System.out.println("---------Finish populating relations----------\n");
		
		System.out.println("[MergeSort Algorithm]");
		mergeSortRIO = algo.mergeSortRelation(relR);
		System.out.println("Number of Disks I/O for MergeSort on " + relR.name + ": " + mergeSortRIO + "\n");
		mergeSortSIO = algo.mergeSortRelation(relS);
		System.out.println("Number of Disks I/O for MergeSort on " + relS.name + ": " + mergeSortSIO + "\n");
		
		System.out.println("[Hash Join Algorithm]");
		relRS = new Relation("RelRS");
		hashJoinIO = algo.hashJoinRelations(relR, relS, relRS);
		System.out.println("Number of Disks I/O for Hash Join: " + hashJoinIO + "\n");
		
		System.out.println("[Refined Sort Merge Join Algorithm]");
		relRS = new Relation("RelRS");
		refinedSortMergeJoinIO = algo.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("Number of Disks I/O for Refined Sort Merge Join: " + refinedSortMergeJoinIO + "\n");
		System.out.println("------------------------------------------------------------");
		
		
		// ------------------------------------------------------------------------------------

		System.out.println("(Test Case 3)");
		Setting.blockFactor = 10;
		Setting.memorySize = 13;
		System.out.println("The block factor is " + Setting.blockFactor);
		System.out.println("The memory size is " + Setting.memorySize);

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		System.out.println("Relation RelR contains " + relR.getNumBlocks() + " blocks."); 
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("Relation RelS contains " + relS.getNumBlocks() + " blocks.");
		System.out.println("---------Finish populating relations----------\n");

		System.out.println("[MergeSort Algorithm]");
		mergeSortRIO = algo.mergeSortRelation(relR);
		System.out.println("Number of Disks I/O for MergeSort on " + relR.name + ": " + mergeSortRIO + "\n");
		mergeSortSIO = algo.mergeSortRelation(relS);
		System.out.println("Number of Disks I/O for MergeSort on " + relS.name + ": " + mergeSortSIO + "\n");

		System.out.println("[Hash Join Algorithm]");
		relRS = new Relation("RelRS");
		hashJoinIO = algo.hashJoinRelations(relR, relS, relRS);
		System.out.println("Number of Disks I/O for Hash Join: " + hashJoinIO + "\n");

		System.out.println("[Refined Sort Merge Join Algorithm]");
		relRS = new Relation("RelRS");
		refinedSortMergeJoinIO = algo.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("Number of Disks I/O for Refined Sort Merge Join: " + refinedSortMergeJoinIO + "\n");
		System.out.println("------------------------------------------------------------");
	}

	/**
	 * This main method provided for testing purpose
	 * 
	 * @param arg
	 */
	public static void main(String[] arg) {
		testCases();
	}
}
