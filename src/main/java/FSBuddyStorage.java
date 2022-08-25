import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.smartfoxserver.v2.buddylist.storage.BuddyStorage;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.commons.io.FileUtils;

import com.smartfoxserver.v2.buddylist.Buddy;
import com.smartfoxserver.v2.buddylist.BuddyList;
import com.smartfoxserver.v2.buddylist.BuddyListManager;
import com.smartfoxserver.v2.buddylist.BuddyVariable;
import com.smartfoxserver.v2.buddylist.SFSBuddy;
import com.smartfoxserver.v2.buddylist.SFSBuddyList;
import com.smartfoxserver.v2.buddylist.SFSBuddyVariable;
import com.smartfoxserver.v2.core.SFSConstants;
import com.smartfoxserver.v2.entities.data.ISFSArray;
import com.smartfoxserver.v2.entities.data.ISFSObject;
import com.smartfoxserver.v2.entities.data.SFSArray;
import com.smartfoxserver.v2.entities.data.SFSObject;
import com.smartfoxserver.v2.exceptions.SFSBuddyListException;
import com.smartfoxserver.v2.exceptions.SFSBuddyListNotFoundException;
import com.smartfoxserver.v2.util.CryptoUtils;

/**
 * Default (file system based) BuddyStorage class
 * <p>
 * Stores each buddy list under the {SFS2X-folder}/data/buddylists/{Zonename}/{Username} folder<br>
 * Where {Username} is the "encoded" version of the User name. The encoded name consist simply of the hexadecimal version
 * of the name. This allows to store any user name in the file system including those containing avoid characters (such as *, :, ? etc...)
 *  
 */
public class FSBuddyStorage implements BuddyStorage
{
	private static final String BL_ROOT_FOLDER = "buddylists/";
	
	private static final String KEY_LIST_OWNER = "lo";
	private static final String KEY_MY_VARIABLES = "mv";
	private static final String KEY_BUDDY_LIST = "bl";
	private static final String KEY_BUDDY_NAME = "bn";
	private static final String KEY_BUDDY_BLOCK = "bb";
	
	private final Logger log;
	private BuddyListManager buddyListManager;
	private String blistFolderName;
	private boolean inited = false;
	
	public FSBuddyStorage()
    {
		log = LoggerFactory.getLogger(this.getClass());
	}
	
	@Override
	public void init()
	{
		try
		{
			checkFolderStructure();
			inited = true;
		}
		catch(IOException ioErr)
		{
			log.warn("Was not able to initialize BuddyStorage: " + ioErr);
		}
	}
	
	@Override
	public void destroy()
	{
	}
	
	/*
	 * For reference on the Data Structure of the Serialized BuddyList see saveList() method
	 * 
	 */
	@Override
    public BuddyList loadList(String ownerName) throws SFSBuddyListNotFoundException, IOException
    {
		checkInited();
		
		// Check file existence
		String targetFileName = blistFolderName + "/" + CryptoUtils.getHexFileName(ownerName);
	    File targetFile = new File(targetFileName);

	    if (!targetFile.isFile())
	    	throw new SFSBuddyListNotFoundException("BuddyList not found for: " + ownerName);

	    // Load binary data and rebuild object
	    ISFSObject serializedBuddyList = SFSObject.newFromBinaryData( FileUtils.readFileToByteArray(targetFile) );
	    
	    // Prepare the buddy list
	    BuddyList buddyList = new SFSBuddyList(serializedBuddyList.getUtfString(KEY_LIST_OWNER), buddyListManager);
	    
	    // This is the array of serialized buddies
	    ISFSArray buddyArray = serializedBuddyList.getSFSArray(KEY_BUDDY_LIST);

	    for (int i = 0; i < buddyArray.size(); i++)
	    {
	    	ISFSObject buddyItem = buddyArray.getSFSObject(i);
	    	Buddy buddy = new SFSBuddy(buddyItem.getUtfString(KEY_BUDDY_NAME));
	    	buddy.setParentBuddyList(buddyList);
	    	
	    	buddy.setBlocked(buddyItem.getBool(KEY_BUDDY_BLOCK));
	    	
	    	/*
	    	 *  Catch exceptions inside loop
	    	 *  
	    	 *  NOTE: This problem should never appear (loading and failing the max buddy list size)
	    	 *  It could happen if the user reduces the size after some time.
	    	 *  In that case the system is not able to load all the buddies in the buddy list.
	    	 *  
	    	 */
	    	try
            {
	            buddyList.addBuddy(buddy);
            }
            catch (SFSBuddyListException e)
            {
	            // TODO what to do? :D
            	// We skip adding... it's the only way. If the size of the list has been reduced we don't load
            	// the excessive buddies. On the next save everything will go back in place.
            }
	    }
	    
	    return buddyList;
    }
	
	
	/*
	 * Data Structure Graph:
	 * 
	 * BuddyList
	 * 		+ owner(String)							// Buddy List owner name
	 * 		+ buddyList(SFSArray):					// List of buddies (name + block)
	 * 			+ Buddy(SFSObject):
	 * 				+ name(String)
	 * 				+ block(Bool)
	 * 			...
	 * 			...
	 * 		+ buddyVars(SFSArray):					// List of BuddyVariables (name + type + val)
	 * 			+ BuddyVar(SFSArray):
	 * 				+ 0: name
	 * 				+ 1: type	
	 * 				+ 2: val
	 */
	@Override
    public void saveList(BuddyList buddyList) throws IOException
    {
		checkInited();
		
		ISFSObject serializedBuddyList = SFSObject.newInstance();
		ISFSArray buddyArray = SFSArray.newInstance();
		ISFSArray buddyVarsArray = SFSArray.newInstance();
		
		// Populate Buddies
		for (Buddy buddy : buddyList.getBuddies())
		{
			// Skip temp buddies
			if (buddy.isTemp())
				continue;
			
			ISFSObject buddyObj = SFSObject.newInstance();
			buddyObj.putUtfString(KEY_BUDDY_NAME, buddy.getName());
			buddyObj.putBool(KEY_BUDDY_BLOCK, buddy.isBlocked());
			
			buddyArray.addSFSObject(buddyObj);
		}
		
		// Populate off-line Buddy Variables ( if supported )
		if (buddyListManager.allowOfflineBuddyVariables())
		{
			List<BuddyVariable> myBuddyVars = buddyList.getOwner().getBuddyProperties().getVariables();
			
			if (myBuddyVars != null)
			{
				for (BuddyVariable bv : myBuddyVars)
				{
					if (bv.isOffline())
						buddyVarsArray.addSFSArray(bv.toSFSArray());
				}
			}
		}
		
		serializedBuddyList.putUtfString(KEY_LIST_OWNER, buddyList.getOwnerName());
		serializedBuddyList.putSFSArray(KEY_BUDDY_LIST, buddyArray);
		serializedBuddyList.putSFSArray(KEY_MY_VARIABLES, buddyVarsArray);
		
		// Save to disk
		String fileName = CryptoUtils.getHexFileName(buddyList.getOwnerName());
		byte[] data = serializedBuddyList.toBinary();
		
		//System.out.println("SERIALIZED BUDDY LIST\n" + serializedBuddyList.getDump());
		
		FileUtils.writeByteArrayToFile
		(
			new File(blistFolderName + "/" + fileName), 
			data
		);
		
		if (log.isDebugEnabled())
			log.debug("BuddyList saved: " + buddyList.getOwnerName() + ", " + data.length + " bytes written.");
    }
	
	@Override
    public List<BuddyVariable> getOfflineVariables(String buddyName) throws IOException
    {
		checkInited();
		
		List<BuddyVariable> offlineBuddyVars = null;
		
		// Is this supported?
		if (buddyListManager.allowOfflineBuddyVariables())
		{
			// Check file existence
			String targetFileName = blistFolderName + "/" + CryptoUtils.getHexFileName(buddyName);
		    File targetFile = new File(targetFileName);
		    
		    if (targetFile.isFile())
		    {
		    	// Grab binary data and rebuild SFSObject
		    	ISFSObject serializedBuddyList = SFSObject.newFromBinaryData( FileUtils.readFileToByteArray(targetFile) );
		    	offlineBuddyVars = rebuildBuddyVariables(serializedBuddyList);
		    }
		}
		
		return offlineBuddyVars;
    }
	
	@Override
	public BuddyListManager getBuddyListManager()
	{
	    return buddyListManager;
	}
	
	@Override
	public void setBuddyListManager(BuddyListManager buddyListManager)
	{
		if (this.buddyListManager != null)
			throw new IllegalStateException("Can't re-assign buddyListManager.");
		
		this.buddyListManager = buddyListManager;
		
	}
	
	// ::: Private Methods ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	
	private void checkFolderStructure() throws IOException
	{
		blistFolderName = SFSConstants.STORAGE_DATA_FOLDER + BL_ROOT_FOLDER + buddyListManager.getZone().getName();
		File targetFolder = new File(blistFolderName);
		
		// If BuddyList folder doesn't exist create it.
		if (!targetFolder.isDirectory())
			FileUtils.forceMkdir(targetFolder);
	}
	
	private void checkInited()
	{
		if (!inited)
			throw new IllegalStateException("BuddyStorage class cannot operate correctly because initialization failed: " +  buddyListManager.getZone());
	}
	
	private List<BuddyVariable> rebuildBuddyVariables(ISFSObject serializedBuddyList)
	{
		List<BuddyVariable> buddyVariables = new ArrayList<BuddyVariable>();
		
		ISFSArray varItems = serializedBuddyList.getSFSArray(KEY_MY_VARIABLES);
    	
    	// Rebuild variables
    	for (int i = 0; i < varItems.size(); i++)
    	{
    		buddyVariables.add( SFSBuddyVariable.newFromSFSArray(varItems.getSFSArray(i)));
    	}
    	
		return buddyVariables;
	}
}
