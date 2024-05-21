    while (true) {
        // Check if there is enough space to insert the key and RID
        if (offset + sizeof(int) + stringLength + sizeof(RID) > PAGE_SIZE) {
            break;
        }
        // Copy the string length
        memcpy((char *)data + offset, &stringLength, sizeof(int));
        offset += sizeof(int);

        // Copy the string data
        memcpy((char *)data + offset, word, stringLength);
        offset += stringLength;

        // Copy the RID
        memcpy((char *)data + offset, &rid, sizeof(RID));
        offset += sizeof(RID);

        // Update the number of entries and free space offset in the header
        leafPageHeader.numEntries += 1;
        leafPageHeader.freeSpaceOffset = offset;
    }